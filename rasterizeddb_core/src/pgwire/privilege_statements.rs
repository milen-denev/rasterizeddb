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

/// What a GRANT or REVOKE acts on: each object's ACL key with the name to
/// show for it, the privileges it takes, and the word for its kind.
struct Resolved {
    keys: Vec<(String, String)>,
    valid: &'static [&'static str],
    label: &'static str,
}

/// `target`'s objects, each checked to exist; `None` for one nothing is
/// enforced on (a schema, a database, a type).
async fn resolve_target(database: &Arc<Database>, target: Target, tx: Option<TxId>) -> Result<Option<Resolved>, (&'static str, String)> {
    use crate::core::rql::functions::{self, RoutineKind};
    let pair = |n: String| (n.clone(), n);
    Ok(Some(match target {
        Target::Other(_) => return Ok(None),
        Target::Topics(names) => broker_target(database, false, Some(names))?,
        Target::AllTopics => broker_target(database, false, None)?,
        Target::Exchanges(names) => broker_target(database, true, Some(names))?,
        Target::AllExchanges => broker_target(database, true, None)?,
        Target::AllTablesInSchema => {
            let mut all: Vec<String> = database.relation_names().into_iter().filter(|n| !n.starts_with("__")).collect();
            all.extend(crate::core::rql::views::load(database, None, tx).await.unwrap_or_default().into_iter().map(|v| v.name));
            Resolved { keys: all.into_iter().map(pair).collect(), valid: privileges::TABLE_PRIVILEGES, label: "table" }
        }
        Target::Tables(names) => {
            for table in &names {
                if !relation_exists(database, table, tx).await {
                    return Err(("42P01", format!("relation \"{table}\" does not exist")));
                }
            }
            Resolved { keys: names.into_iter().map(pair).collect(), valid: privileges::TABLE_PRIVILEGES, label: "table" }
        }
        Target::Sequences(names) => {
            for name in &names {
                if !crate::core::rql::sequence_catalog::exists(database, name, tx).await {
                    return Err(("42P01", format!("relation \"{name}\" does not exist")));
                }
            }
            Resolved { keys: names.into_iter().map(pair).collect(), valid: privileges::SEQUENCE_PRIVILEGES, label: "sequence" }
        }
        Target::AllSequencesInSchema => {
            let all = crate::core::rql::sequence_catalog::load(database, None, tx).await.map_err(|e| ("XX000", e))?;
            Resolved { keys: all.into_iter().map(|s| pair(s.name)).collect(), valid: privileges::SEQUENCE_PRIVILEGES, label: "sequence" }
        }
        Target::Routines { kind, names } => {
            let mut keys = Vec::new();
            for name in names {
                let found = functions::load(database, Some(&name), tx).await.map_err(|e| ("XX000", e))?;
                let matching: Vec<&str> = found
                    .iter()
                    .map(|r| if r.kind == RoutineKind::Procedure { "procedure" } else { "function" })
                    .filter(|k| kind == "routine" || *k == kind)
                    .collect();
                let Some(k) = matching.first() else {
                    return Err(("42883", format!("{} {name} does not exist", if kind == "routine" { "function" } else { kind })));
                };
                keys.push((format!("@{k}:{name}"), name));
            }
            Resolved { keys, valid: privileges::ROUTINE_PRIVILEGES, label: "function" }
        }
        Target::AllRoutinesInSchema { kind } => {
            let all = functions::load(database, None, tx).await.map_err(|e| ("XX000", e))?;
            let keys = all
                .into_iter()
                .filter_map(|r| {
                    let k = if r.kind == RoutineKind::Procedure { "procedure" } else { "function" };
                    (kind == "routine" || k == kind).then(|| (format!("@{k}:{}", r.name), r.name))
                })
                .collect();
            Resolved { keys, valid: privileges::ROUTINE_PRIVILEGES, label: "function" }
        }
    }))
}

/// A GRANT's or REVOKE's broker objects (`TOPIC`, `QUEUE`, `EXCHANGE`, `ALL
/// TOPICS`, `ALL EXCHANGES`), each checked to exist. `ALL TOPICS` leaves out
/// the hidden ones (`$…`, the MQTT front end's sessions and retained
/// messages), which are granted by name if at all.
fn broker_target(database: &Arc<Database>, exchange: bool, names: Option<Vec<String>>) -> Result<Resolved, (&'static str, String)> {
    let (valid, label) = if exchange {
        (privileges::EXCHANGE_PRIVILEGES, "exchange")
    } else {
        (privileges::TOPIC_PRIVILEGES, "topic")
    };
    #[cfg(feature = "broker")]
    {
        let broker = database.broker.get().map_err(|e| ("XX000", e.message))?;
        let names = match names {
            Some(names) => {
                for n in &names {
                    let there = if exchange { broker.exchange_kind(n).is_some() } else { broker.topic(n).is_some() };
                    if !there {
                        return Err(("42704", format!("{label} \"{n}\" does not exist")));
                    }
                }
                names
            }
            None if exchange => broker.exchanges().into_iter().map(|x| x.0).collect(),
            None => broker.topics().iter().map(|t| t.name.clone()).filter(|n| !n.starts_with('$')).collect(),
        };
        let keys = names
            .into_iter()
            .map(|n| (if exchange { privileges::exchange_key(&n) } else { privileges::topic_key(&n) }, n))
            .collect();
        Ok(Resolved { keys, valid, label })
    }
    #[cfg(not(feature = "broker"))]
    {
        let _ = (database, valid);
        match names.and_then(|n| n.into_iter().next()) {
            Some(n) => Err(("42704", format!("{label} \"{n}\" does not exist"))),
            None => Ok(Resolved { keys: Vec::new(), valid, label }),
        }
    }
}

/// Whether the broker object an ACL key names (`@topic:x`, `@exchange:x`)
/// exists; `None` for a key that names no broker object.
fn broker_object_exists(database: &Arc<Database>, key: &str) -> Option<bool> {
    let (exchange, name) = if let Some(n) = key.strip_prefix("@topic:") {
        (false, n)
    } else {
        (true, key.strip_prefix("@exchange:")?)
    };
    #[cfg(feature = "broker")]
    {
        let Ok(broker) = database.broker.get() else { return Some(false) };
        Some(if exchange { broker.exchange_kind(name).is_some() } else { broker.topic(name).is_some() })
    }
    #[cfg(not(feature = "broker"))]
    {
        let _ = (database, exchange, name);
        Some(false)
    }
}

/// A `BROKER` statement run as this session's role (`broker::acl`): each
/// topic and exchange it touches is checked, and what it creates is owned
/// by the role, recorded before the statement's reply. The owner rows are
/// written outside the session's transaction: a broker object is not
/// transactional, and a rollback that took its owner row would leave it
/// to superusers.
#[cfg(feature = "broker")]
pub(super) async fn broker_statement(database: &Arc<Database>, state: &SessionState, sql: &str) -> Vec<u8> {
    use crate::core::broker::acl::{self, Access};
    let me = state.principal.name.clone();
    // A superuser is asked nothing, but is given the rows too:
    // `BROKER SHOW GRANTS` lists them.
    let mut access = if state.principal.may_administer() { Access::unrestricted(&me) } else { Access::new(&me, false) };
    match privileges::load_shared(database, state.current_tx()).await {
        Ok(rows) => access.set_rows(rows, None),
        Err(e) => {
            return crate::core::introspect::failure(&format!(
                "permission denied: the privilege catalogue could not be read ({e})"
            ));
        }
    }
    let (out, effects) = crate::core::broker::sql::execute_as(sql, &database.broker, &mut access);
    for (kind, name, created) in effects {
        if created {
            acl::record_created(database, kind, &name, &me).await;
        } else {
            acl::record_removed(database, kind, &name).await;
        }
    }
    out
}

/// Whether `me` may grant or revoke membership in `roles` (R2): a
/// superuser; a CREATEROLE role, for roles that are not superusers; or a
/// holder of ADMIN OPTION on each.
async fn may_manage_membership(
    database: &Arc<Database>,
    rows: &[AclRow],
    me: &str,
    superuser: bool,
    roles: &[String],
    verb: &str,
    tx: Option<TxId>,
) -> Result<(), (&'static str, String)> {
    if superuser {
        return Ok(());
    }
    let create_role = crate::auth::role_attrs(database, me, tx).await.create_role;
    for role in roles {
        if crate::auth::lookup_role_in(database, role, tx).await.is_some_and(|r| r.is_superuser) {
            return Err(("42501", format!("permission denied to {verb} role \"{role}\": only a superuser can {verb} a superuser role")));
        }
        if !create_role && !privileges::has_admin_option(rows, me, role) {
            return Err(("42501", format!("permission denied to {verb} role \"{role}\": it needs ADMIN OPTION on it, or CREATEROLE")));
        }
    }
    Ok(())
}

/// Writes (`on`) or removes a membership's companion row (`NOINHERIT`,
/// `NOSET`).
async fn set_companion(
    database: &Arc<Database>,
    relation: &str,
    member: &str,
    companion: &str,
    on: bool,
    me: &str,
    tx: Option<TxId>,
) -> Result<(), (&'static str, String)> {
    if on {
        let row = AclRow { relation: relation.into(), grantee: member.into(), privilege: companion.into(), grantor: me.into(), grantable: false };
        privileges::put(database, &row, tx).await.map_err(|e| ("XX000", e))
    } else {
        let filter = format!(
            "relation = {} AND grantee = {} AND privilege = {}",
            crate::auth::quote(relation),
            crate::auth::quote(member),
            crate::auth::quote(companion)
        );
        privileges::delete_where(database, &filter, tx).await.map_err(|e| ("XX000", e))
    }
}

async fn role_exists(database: &Arc<Database>, name: &str, tx: Option<TxId>) -> bool {
    name == "public" || crate::auth::lookup_role_in(database, name, tx).await.is_some()
}

/// `CREATE/ALTER/DROP POLICY` and `ALTER TABLE … ROW LEVEL SECURITY`
/// (R3): the table's owner's, or a superuser's.
pub(super) async fn handle_policy_statement<S>(
    database: &Arc<Database>,
    state: &mut SessionState,
    stream: &mut S,
    parsed: Result<crate::core::rql::policies::PolicyStatement, crate::core::rql::policies::Refusal>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::core::rql::policies::{self, PolicyStatement};
    let statement = match parsed {
        Ok(statement) => statement,
        Err((code, message)) => return fail_statement(state, stream, code, &message).await,
    };
    let tx = state.current_tx();
    let table = match &statement {
        PolicyStatement::Create(p) => p.table.clone(),
        PolicyStatement::Alter { table, .. } | PolicyStatement::Drop { table, .. } | PolicyStatement::Toggle { table, .. } => table.clone(),
    };
    if !relation_exists(database, &table, tx).await || crate::core::rql::views::lookup(database, &table, tx).await.ok().flatten().is_some() {
        return fail_statement(state, stream, "42P01", &format!("relation \"{table}\" does not exist or is not a table")).await;
    }
    if !state.principal.may_administer() {
        let owns = privileges::load_shared(database, tx).await.is_ok_and(|rows| privileges::owns(&rows, &state.principal.name, &table));
        if !owns {
            return fail_statement(state, stream, "42501", &format!("must be owner of table {table}")).await;
        }
    }
    let existing = match policies::load(database, Some(&table), tx).await {
        Ok(p) => p,
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    let result: Result<&'static str, String> = async {
        match statement {
            PolicyStatement::Create(policy) => {
                if existing.iter().any(|p| p.name == policy.name) {
                    return Err(format!("policy \"{}\" for table \"{table}\" already exists", policy.name));
                }
                for role in policy.roles.iter().filter(|r| *r != "public") {
                    if !role_exists(database, role, tx).await {
                        return Err(format!("role \"{role}\" does not exist"));
                    }
                }
                policies::store(database, &policy, tx).await?;
                Ok("CREATE POLICY")
            }
            PolicyStatement::Alter { name, rename, roles, using, check, .. } => {
                let Some(mut policy) = existing.iter().find(|p| p.name == name).cloned() else {
                    return Err(format!("policy \"{name}\" for table \"{table}\" does not exist"));
                };
                if let Some(roles) = roles {
                    policy.roles = roles;
                }
                if using.is_some() {
                    policy.using = using;
                }
                if check.is_some() {
                    policy.check = check;
                }
                if let Some(new) = rename {
                    if existing.iter().any(|p| p.name == new) {
                        return Err(format!("policy \"{new}\" for table \"{table}\" already exists"));
                    }
                    policies::remove(database, &table, Some(&name), tx).await?;
                    policy.name = new;
                }
                policies::store(database, &policy, tx).await?;
                Ok("ALTER POLICY")
            }
            PolicyStatement::Drop { name, if_exists, .. } => {
                if !existing.iter().any(|p| p.name == name) {
                    return if if_exists { Ok("DROP POLICY") } else { Err(format!("policy \"{name}\" for table \"{table}\" does not exist")) };
                }
                policies::remove(database, &table, Some(&name), tx).await?;
                Ok("DROP POLICY")
            }
            PolicyStatement::Toggle { marker, on, .. } => {
                let me = state.principal.name.clone();
                set_companion(database, &table, "-", marker, on, &me, tx).await.map_err(|(_, e)| e)?;
                Ok("ALTER TABLE")
            }
        }
    }
    .await;
    match result {
        Ok(tag) => send_command_complete(stream, tag).await,
        Err(e) => fail_statement(state, stream, sqlstate_for(&e), &e).await,
    }
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
    let rows = match privileges::load_shared(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return fail_statement(state, stream, "XX000", &e).await,
    };
    let fail = |code: &'static str, message: String| (code, message);
    let result: Result<&'static str, (&'static str, String)> = async {
        match statement {
            PrivilegeStatement::Grant { privileges: privs, target, grantees, grantable } => {
                let Some(Resolved { keys: tables, valid, label }) = resolve_target(database, target, tx).await? else {
                    return Ok("GRANT");
                };
                let privs = privileges::privileges_for(privs, valid)
                    .map_err(|p| fail("0LP01", format!("invalid privilege type {p} for {label}")))?;
                for grantee in &grantees {
                    if !role_exists(database, grantee, tx).await {
                        return Err(fail("42704", format!("role \"{grantee}\" does not exist")));
                    }
                }
                for (table, shown) in &tables {
                    for p in &privs {
                        if !admin && !privileges::may_grant(&rows, &me, table, privileges::column_privilege(p).map_or(p.as_str(), |(base, _)| base)) {
                            return Err(fail("42501", format!("permission denied for {label} {shown}")));
                        }
                        if let Some((base, columns)) = privileges::column_privilege(p) {
                            if label != "table" {
                                return Err(fail("0LP01", format!("column privileges are for tables, not a {label}")));
                            }
                            if !admin && !privileges::may_grant(&rows, &me, table, base) {
                                return Err(fail("42501", format!("permission denied for {label} {shown}")));
                            }
                            for column in columns {
                                if !column_exists(database, table, column) {
                                    return Err(fail("42703", format!("column \"{column}\" of relation \"{shown}\" does not exist")));
                                }
                                for grantee in &grantees {
                                    let row = AclRow { relation: table.clone(), grantee: grantee.clone(), privilege: privileges::column_key(base, column), grantor: me.clone(), grantable };
                                    privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                            }
                            continue;
                        }
                        for grantee in &grantees {
                            // PUBLIC's EXECUTE is the default: granting it
                            // back takes the marker away.
                            if p == "EXECUTE" && grantee == "public" {
                                set_companion(database, table, "public", privileges::NOEXECUTE, false, &me, tx).await?;
                                continue;
                            }
                            let row = AclRow { relation: table.clone(), grantee: grantee.clone(), privilege: p.clone(), grantor: me.clone(), grantable };
                            privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                        }
                    }
                }
                Ok("GRANT")
            }
            PrivilegeStatement::Revoke { privileges: privs, target, grantees, grant_option_only, cascade } => {
                let Some(Resolved { keys: tables, valid, label }) = resolve_target(database, target, tx).await? else {
                    return Ok("REVOKE");
                };
                let privs = privileges::privileges_for(privs, valid)
                    .map_err(|p| fail("0LP01", format!("invalid privilege type {p} for {label}")))?;
                for (table, shown) in &tables {
                    for p in &privs {
                        if !admin && !privileges::may_grant(&rows, &me, table, privileges::column_privilege(p).map_or(p.as_str(), |(base, _)| base)) {
                            return Err(fail("42501", format!("permission denied for {label} {shown}")));
                        }
                        if let Some((base, columns)) = privileges::column_privilege(p) {
                            for column in columns {
                                for grantee in &grantees {
                                    let filter = format!(
                                        "relation = {} AND grantee = {} AND privilege = {}",
                                        crate::auth::quote(table),
                                        crate::auth::quote(grantee),
                                        crate::auth::quote(&privileges::column_key(base, column))
                                    );
                                    privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                            }
                            continue;
                        }
                        for grantee in &grantees {
                            if p == "EXECUTE" && grantee == "public" && !grant_option_only {
                                set_companion(database, table, "public", privileges::NOEXECUTE, true, &me, tx).await?;
                                continue;
                            }
                            let existing = rows.iter().find(|r| r.relation == *table && r.grantee == *grantee && r.privilege == *p);
                            // What the grantee passed on with its grant
                            // option goes with it, or stops the REVOKE.
                            let dependents = privileges::dependent_grants(&rows, table, p, grantee);
                            if existing.is_some() && !dependents.is_empty() {
                                if !cascade {
                                    return Err(fail(
                                        "2BP01",
                                        format!(
                                            "dependent privileges exist: {grantee} granted {p} on {shown} to {}; use CASCADE to revoke them too",
                                            dependents.join(", ")
                                        ),
                                    ));
                                }
                                for d in &dependents {
                                    let filter = format!(
                                        "relation = {} AND grantee = {} AND privilege = {}",
                                        crate::auth::quote(table),
                                        crate::auth::quote(d),
                                        crate::auth::quote(p)
                                    );
                                    privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                            }
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
            PrivilegeStatement::GrantRole { roles, members, admin: with_admin, inherit, set } => {
                for name in roles.iter().chain(&members) {
                    if name == "public" || !role_exists(database, name, tx).await {
                        return Err(fail("42704", format!("role \"{name}\" does not exist")));
                    }
                }
                may_manage_membership(database, &rows, &me, admin, &roles, "grant", tx).await?;
                for role in &roles {
                    let relation = format!("@role:{role}");
                    for member in &members {
                        if privileges::roles_of(&rows, role).contains(member) || privileges::settable_roles(&rows, role).contains(member) {
                            return Err(fail("0LP01", format!("role \"{member}\" is a member of role \"{role}\"")));
                        }
                        // Granted again without ADMIN, a membership keeps the
                        // ADMIN option it had, as in PostgreSQL.
                        let had_admin = rows.iter().any(|r| r.privilege == "MEMBER" && r.relation == relation && r.grantee == *member && r.grantable);
                        let row = AclRow { relation: relation.clone(), grantee: member.clone(), privilege: "MEMBER".into(), grantor: me.clone(), grantable: with_admin || had_admin };
                        privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                        for (option, companion) in [(inherit, privileges::NOINHERIT), (set, privileges::NOSET)] {
                            if let Some(on) = option {
                                set_companion(database, &relation, member, companion, !on, &me, tx).await?;
                            }
                        }
                    }
                }
                Ok("GRANT ROLE")
            }
            PrivilegeStatement::RevokeRole { roles, members, option } => {
                may_manage_membership(database, &rows, &me, admin, &roles, "revoke", tx).await?;
                for role in &roles {
                    let relation = format!("@role:{role}");
                    for member in &members {
                        match option {
                            None => {
                                let filter = format!(
                                    "relation = {} AND grantee = {} AND (privilege = 'MEMBER' OR privilege = {} OR privilege = {})",
                                    crate::auth::quote(&relation),
                                    crate::auth::quote(member),
                                    crate::auth::quote(privileges::NOINHERIT),
                                    crate::auth::quote(privileges::NOSET),
                                );
                                privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                            }
                            Some(privileges::MembershipOption::Admin) => {
                                if let Some(r) = rows.iter().find(|r| r.privilege == "MEMBER" && r.relation == relation && r.grantee == *member) {
                                    let row = AclRow { grantable: false, ..r.clone() };
                                    privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                                }
                            }
                            Some(privileges::MembershipOption::Inherit) => {
                                set_companion(database, &relation, member, privileges::NOINHERIT, true, &me, tx).await?;
                            }
                            Some(privileges::MembershipOption::Set) => {
                                set_companion(database, &relation, member, privileges::NOSET, true, &me, tx).await?;
                            }
                        }
                    }
                }
                Ok("REVOKE ROLE")
            }
            PrivilegeStatement::DefaultPrivileges { for_role, grant, privileges: privs, grantees, kind } => {
                let role = for_role.unwrap_or_else(|| me.clone());
                if !admin && role != me {
                    return Err(fail("42501", "permission denied to change default privileges".to_string()));
                }
                if kind == privileges::DefaultKind::Other {
                    return Ok("ALTER DEFAULT PRIVILEGES");
                }
                let relation = kind.relation(&role);
                for grantee in &grantees {
                    for p in &privs {
                        // PUBLIC's EXECUTE: the default is to have it, so
                        // a REVOKE is a marker the creation copies.
                        if p == "EXECUTE" && grantee == "public" {
                            set_companion(database, &relation, "public", privileges::NOEXECUTE, !grant, &role, tx).await?;
                            continue;
                        }
                        if grant {
                            let row = AclRow { relation: relation.clone(), grantee: grantee.clone(), privilege: p.clone(), grantor: role.clone(), grantable: false };
                            privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                        } else {
                            let filter = format!(
                                "relation = {} AND grantee = {} AND privilege = {}",
                                crate::auth::quote(&relation),
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
                let broker_object = broker_object_exists(database, &relation);
                // `topic orders`, `exchange events`, or `table t`.
                let shown = match relation.strip_prefix('@').and_then(|k| k.split_once(':')) {
                    Some((kind, name)) if broker_object.is_some() => format!("{kind} {name}"),
                    _ => format!("table {relation}"),
                };
                if broker_object == Some(false) {
                    return Err(fail("42704", format!("{shown} does not exist")));
                }
                if broker_object.is_none() && !relation_exists(database, &relation, tx).await {
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
                        return Err(fail("42501", format!("must be owner of {shown}")));
                    }
                    if !privileges::roles_of(&rows, &me).iter().any(|r| *r == owner) {
                        return Err(fail("42501", format!("must be able to SET ROLE \"{owner}\"")));
                    }
                }
                let filter = format!("relation = {} AND privilege = 'OWNER'", crate::auth::quote(&relation));
                privileges::delete_where(database, &filter, tx).await.map_err(|e| fail("XX000", e))?;
                let row = AclRow { relation, grantee: owner, privilege: "OWNER".into(), grantor: me.clone(), grantable: false };
                privileges::put(database, &row, tx).await.map_err(|e| fail("XX000", e))?;
                Ok(match broker_object {
                    Some(_) if shown.starts_with("exchange") => "ALTER EXCHANGE",
                    Some(_) => "ALTER TOPIC",
                    None => "ALTER TABLE",
                })
            }
            PrivilegeStatement::ReassignOwned { from, to } => {
                if !role_exists(database, &to, tx).await {
                    return Err(fail("42704", format!("role \"{to}\" does not exist")));
                }
                // As PostgreSQL asks: the giver acts as every role named.
                let mine = privileges::settable_roles(&rows, &me);
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
                let mine = privileges::settable_roles(&rows, &me);
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
                    for relation in privileges::default_relations(r) {
                        let defaults = format!("relation = {}", crate::auth::quote(&relation));
                        privileges::delete_where(database, &defaults, tx).await.map_err(|e| fail("XX000", e))?;
                    }
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
    // `pg_authid`, as in PostgreSQL, is a superuser's to read.
    if crate::core::sql_scan::contains_ignore_ascii_case(stmt, b"pg_authid")
        && crate::core::rql::lock_set::named_words(stmt)
            .iter()
            .any(|w| !w.alias && w.name.rsplit('.').next().unwrap_or(&w.name).eq_ignore_ascii_case("pg_authid"))
    {
        return Some(("42501", "permission denied for table pg_authid".to_string()));
    }
    let rows = match privileges::load_shared(database, tx).await {
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
        if !privileges::allows(&rows, me, &r.name, privilege)
            && (view.is_some() || !columns_suffice(database, &rows, me, &r.name, privilege, stmt, r.span.end))
        {
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

/// Whether `table` has a live column `column`.
fn column_exists(database: &Arc<Database>, table: &str, column: &str) -> bool {
    database
        .schema_view(table)
        .is_some_and(|v| v.schema().fields.iter().any(|f| !f.is_deleted && f.name == column))
}

/// Whether `role`'s column grants on `table` cover what `stmt` does with it
/// (R2), for a role without the table-wide `privilege`. Conservative: a
/// word of the statement that names a column of the table counts as a use
/// of it, wherever it stands, so an alias spelled as a column may refuse
/// what PostgreSQL allows, never the other way. A `*` (other than
/// `count(*)`) reads every column and needs the table-wide grant; an INSERT
/// names its columns; an UPDATE's SET targets need UPDATE on each, what it
/// reads SELECT or UPDATE; DELETE has no column form.
fn columns_suffice(database: &Arc<Database>, rows: &[AclRow], role: &str, table: &str, privilege: &str, stmt: &str, after_ref: usize) -> bool {
    let granted = privileges::granted_columns(rows, role, table, privilege);
    if granted.is_empty() || privilege == "DELETE" {
        return false;
    }
    let Some(view) = database.schema_view(table) else {
        return false;
    };
    let columns: Vec<String> = view.schema().fields.iter().filter(|f| !f.is_deleted).map(|f| f.name.clone()).collect();
    if star_outside_count(stmt) {
        return false;
    }
    let used: Vec<String> = crate::core::rql::lock_set::named_words(stmt)
        .into_iter()
        .filter(|w| !w.alias)
        .map(|w| w.name)
        .filter(|n| columns.contains(n))
        .collect();
    let selectable = privileges::granted_columns(rows, role, table, "SELECT");
    let readable = |c: &String| granted.contains(c) || selectable.contains(c) || privileges::allows(rows, role, table, "SELECT");
    match privilege {
        "SELECT" => used.iter().all(|c| granted.contains(c)),
        "INSERT" => {
            // `INSERT INTO t (a, b) …`: the list is what is written.
            let rest = stmt.get(after_ref..).unwrap_or("").trim_start();
            let Some(list) = rest.strip_prefix('(') else {
                return false;
            };
            let Some(close) = list.find(')') else {
                return false;
            };
            let targets: Vec<String> = list[..close].split(',').map(|c| object_name(c.trim())).collect();
            targets.iter().all(|c| granted.contains(c)) && used.iter().all(readable)
        }
        "UPDATE" => {
            let Some(targets) = set_targets(stmt) else {
                return false;
            };
            targets.iter().all(|c| granted.contains(c)) && used.iter().all(readable)
        }
        _ => false,
    }
}

/// Whether `stmt` has a `*` outside quotes and comments that is not
/// `count(*)`'s.
fn star_outside_count(stmt: &str) -> bool {
    use crate::core::sql_scan::{skip_comment, skip_quoted};
    let b = stmt.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if let Some(end) = skip_comment(b, i).or_else(|| skip_quoted(b, i)) {
            i = end.max(i + 1);
            continue;
        }
        if b[i] == b'*' {
            let before = stmt[..i].trim_end();
            let after = stmt[i + 1..].trim_start();
            let counted = before.ends_with('(') && after.starts_with(')') && {
                let head = before[..before.len() - 1].trim_end();
                head.len() >= 5 && head[head.len() - 5..].eq_ignore_ascii_case("count")
            };
            // Multiplication, `a * b`, is no column list: a value on both
            // sides, neither a keyword (`DISTINCT *`, `ON (a) * FROM`).
            let last_word = before.rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).next().unwrap_or("").to_ascii_uppercase();
            let next_word = after.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_')).next().unwrap_or("").to_ascii_uppercase();
            let value_before = before.ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == ')' || c == '"')
                && !matches!(last_word.as_str(), "SELECT" | "DISTINCT" | "ALL" | "RETURNING" | "BY" | "ON");
            let value_after = after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == '(' || c == '"' || c == '\'')
                && !matches!(next_word.as_str(), "FROM" | "WHERE" | "RETURNING" | "INTO" | "EXCEPT" | "UNION" | "INTERSECT");
            let arithmetic = value_before && value_after && !before.ends_with(')') || (value_before && value_after && after.starts_with(|c: char| c.is_ascii_digit()));
            if !counted && !arithmetic {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// The columns an UPDATE's SET assigns, `None` when they cannot be read.
fn set_targets(stmt: &str) -> Option<Vec<String>> {
    let at = crate::core::rql::join::find_keyword(stmt, "SET", 0)?;
    let rest = &stmt[at + 3..];
    let end = ["WHERE", "FROM", "RETURNING"]
        .iter()
        .filter_map(|k| crate::core::rql::join::find_keyword(rest, k, 0))
        .min()
        .unwrap_or(rest.len());
    let mut out = Vec::new();
    for assignment in crate::core::sql_scan::split_top_level(&rest[..end], b',') {
        let target = assignment.split('=').next()?.trim();
        // `(a, b) = (…)`.
        if let Some(list) = target.strip_prefix('(') {
            out.extend(list.trim_end_matches(')').split(',').map(|c| object_name(c.trim())));
        } else {
            out.push(object_name(target));
        }
    }
    Some(out)
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
/// The ownership gate of a function, procedure, type, domain or trigger
/// statement, which is answered before [`check`] runs (R1): a role that is
/// not a superuser is refused another's object, and a CREATE records its
/// creator as owner, a superuser's as anyone's. Without it those statements
/// skipped the check entirely: any role could replace or drop any function,
/// and no function had an owner (so none could be SECURITY DEFINER).
pub(super) async fn object_ddl_gate(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<(&'static str, String)> {
    let head: Vec<String> = stmt.split_whitespace().take(4).map(str::to_ascii_uppercase).collect();
    let kind_at = if head.get(1).is_some_and(|w| w == "OR") { 3 } else { 1 };
    let names_one = matches!(head.first().map(String::as_str), Some("CREATE" | "ALTER" | "DROP"))
        && matches!(head.get(kind_at).map(String::as_str), Some("FUNCTION" | "PROCEDURE" | "TYPE" | "DOMAIN" | "TRIGGER"));
    if !names_one || state.view_internal {
        return None;
    }
    let tx = state.current_tx();
    let rows = match privileges::load_shared(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return Some(("42501", format!("permission denied: the privilege catalogue could not be read ({e})"))),
    };
    let refusal = object_ddl_refusal(database, &rows, &state.principal.name, stmt, tx).await;
    if state.principal.may_administer() { None } else { refusal }
}

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
                relation: key.clone(),
                grantee: me.to_string(),
                privilege: "OWNER".to_string(),
                grantor: me.to_string(),
                grantable: false,
            };
            if let Err(e) = privileges::put(database, &row, tx).await {
                return Some(("42501", format!("permission denied: recording the owner of {kind} {name} failed ({e})")));
            }
            // A function or procedure takes its creator's default privileges
            // for functions (R2), PUBLIC's EXECUTE marker among them.
            if kind == "function" || kind == "procedure" {
                let defaults = privileges::DefaultKind::Functions.relation(me);
                for d in rows.iter().filter(|r| r.relation == defaults) {
                    let row = privileges::AclRow { relation: key.clone(), grantor: me.to_string(), grantable: false, ..d.clone() };
                    let _ = privileges::put(database, &row, tx).await;
                }
            }
            None
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

/// The refusal for the `SECURITY DEFINER` functions `stmt` calls, which
/// run with their owners' privileges (R2): the caller needs EXECUTE; each
/// body, its own calls put in as the owner may make them, is read with the
/// owner's SELECT, and a view in it with its own owner's, as a view is
/// (`view_owner_refusal`). Run after the caller's check, which did not see
/// these bodies (`functions::inline_calls` with `invokers_only`).
pub(super) async fn definer_refusal(database: &Arc<Database>, state: &SessionState, stmt: &str) -> Option<(&'static str, String)> {
    use crate::core::rql::functions;
    if state.principal.may_administer() || !functions::may_call_a_routine(database, stmt).await {
        return None;
    }
    let tx = state.current_tx();
    let rows = match privileges::load_shared(database, tx).await {
        Ok(rows) => rows,
        Err(e) => return Some(("42501", format!("permission denied: the privilege catalogue could not be read ({e})"))),
    };
    if !rows.iter().any(|r| r.privilege == privileges::DEFINER) {
        return None;
    }
    let me = state.principal.name.as_str();
    for name in functions::called_routines(database, stmt) {
        let key = format!("@function:{name}");
        if !privileges::is_definer(&rows, &key) {
            continue;
        }
        if !privileges::may_execute(&rows, me, &key) {
            return Some(("42501", format!("permission denied for function {name}")));
        }
        let Some(owner) = privileges::owner_of(&rows, &key) else {
            return Some(("42501", format!("permission denied for function {name}: a SECURITY DEFINER function with no owner")));
        };
        if crate::auth::lookup_role_in(database, owner, tx).await.is_some_and(|r| r.is_superuser) {
            continue;
        }
        let routines = match functions::load(database, Some(&name), tx).await {
            Ok(r) => r,
            Err(e) => return Some(("42501", format!("permission denied: the routine catalogue could not be read ({e})"))),
        };
        for routine in routines {
            let body = match functions::inline_calls(database, &routine.body, tx, Some(owner), false).await {
                Ok(text) => text.unwrap_or(routine.body.clone()),
                Err((code, message)) => return Some((code, message)),
            };
            for r in crate::core::rql::lock_set::relation_references(&body) {
                if r.name.starts_with("__") || !relation_exists(database, &r.name, tx).await {
                    continue;
                }
                if !privileges::allows(&rows, owner, &r.name, "SELECT") {
                    return Some((
                        "42501",
                        format!("permission denied for table {}: the owner of SECURITY DEFINER function {name} does not hold SELECT on it", r.name),
                    ));
                }
                match crate::core::rql::views::lookup(database, &r.name, tx).await {
                    Ok(Some(view)) => {
                        if let Some(refusal) = view_owner_refusal(database, &rows, tx, &view, "SELECT", 0).await {
                            return Some(refusal);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => return Some(("42501", format!("permission denied: the view catalogue could not be read ({e})"))),
                }
            }
        }
    }
    None
}

/// The refusal for a non-superuser's statement on views it does not own:
/// `names` that are views, each its owner's alone to drop, rename, refresh
/// or replace. A name that is no view is the statement's to report.
pub(super) async fn require_view_owner(database: &Arc<Database>, state: &SessionState, names: &[String]) -> Option<(&'static str, String)> {
    if names.is_empty() || state.principal.may_administer() {
        return None;
    }
    let tx = state.current_tx();
    let rows = match privileges::load_shared(database, tx).await {
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
    } else if let Some(Ok(special)) = crate::core::rql::ledger_ddl::split(stmt) {
        // `CREATE LEDGER TABLE` and `CREATE VAULT`: a table like any other to
        // own. Without an owner every grant on it is ignored
        // (`privileges::allows`), so no role but a superuser could read it.
        crate::core::rql::lexer_ct::create_table_with_partition(&crate::core::rql::lexer_s1::QueryPurpose::CreateTable(
            special.create_sql,
        ))
        .0
        .map(|ct| ct.table_name)
        .unwrap_or(special.name)
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
    // A sequence takes its own kind's defaults; a table, a view or a
    // materialized view the tables' (R2).
    let is_sequence = crate::core::rql::sequence_catalog::load(database, Some(&[name.to_string()]), tx)
        .await
        .is_ok_and(|s| !s.is_empty());
    let kind = if is_sequence { privileges::DefaultKind::Sequences } else { privileges::DefaultKind::Tables };
    let relation = kind.relation(&me);
    let defaults: Vec<AclRow> = privileges::load(database, tx)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.relation == relation)
        .collect();
    for d in defaults {
        let row = AclRow { relation: name.to_string(), grantee: d.grantee, privilege: d.privilege, grantor: me.clone(), grantable: false };
        let _ = privileges::put(database, &row, tx).await;
    }
}
