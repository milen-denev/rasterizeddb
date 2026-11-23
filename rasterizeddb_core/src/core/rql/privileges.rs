//! `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, owners, and the check a
//! non-superuser's statement passes.
//!
//! Everything is a row of the system table [`ACL_TABLE`], written in the
//! statement's transaction:
//!
//! - a relation's owner: `(relation, owner, 'OWNER')`;
//! - a grant: `(relation, grantee, privilege)`, `grantee` `public` for
//!   `PUBLIC`;
//! - a role membership (`GRANT role TO member`): `('@role:<role>', member,
//!   'MEMBER')`;
//! - a default privilege (`ALTER DEFAULT PRIVILEGES [FOR ROLE r] GRANT …
//!   ON TABLES`): `('@default:<r>', grantee, privilege)`.
//!
//! A relation created from here on is owned by its creator, and other roles
//! need a grant, as in PostgreSQL. A relation with no owner row (every one
//! created before this existed) is open to every role, as it was. A
//! superuser, and any session under `trust`, passes every check.

use rclite::Arc;

use crate::core::database::Database;
use crate::core::mvcc::TxId;

/// The system table owners, grants, memberships and defaults are kept in.
pub const ACL_TABLE: &str = "__rdb_acl";

const ACL_TABLE_DDL: &str = "CREATE TABLE __rdb_acl (id TEXT PRIMARY KEY, relation TEXT NOT NULL, \
     grantee TEXT NOT NULL, privilege TEXT NOT NULL, grantor TEXT NOT NULL, grantable BOOLEAN NOT NULL)";

pub type Refusal = (&'static str, String);

/// The table privileges PostgreSQL has, in its order.
pub const TABLE_PRIVILEGES: &[&str] = &["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"];

/// A row of [`ACL_TABLE`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclRow {
    pub relation: String,
    pub grantee: String,
    pub privilege: String,
    pub grantor: String,
    pub grantable: bool,
}

/// What a `GRANT` or `REVOKE` is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Tables(Vec<String>),
    AllTablesInSchema,
    /// A sequence, function, schema, database or type: accepted, nothing
    /// enforced for it here.
    Other(String),
}

/// A privilege statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeStatement {
    Grant { privileges: Vec<String>, target: Target, grantees: Vec<String>, grantable: bool },
    Revoke { privileges: Vec<String>, target: Target, grantees: Vec<String>, grant_option_only: bool },
    GrantRole { roles: Vec<String>, members: Vec<String> },
    RevokeRole { roles: Vec<String>, members: Vec<String> },
    /// `ALTER DEFAULT PRIVILEGES [FOR ROLE r] [IN SCHEMA s] GRANT|REVOKE … ON
    /// TABLES …`; `for_role` `None` for the current role. `grant` false for
    /// REVOKE. `tables` false for another object kind (accepted, unenforced).
    DefaultPrivileges { for_role: Option<String>, grant: bool, privileges: Vec<String>, grantees: Vec<String>, tables: bool },
    /// `ALTER TABLE | VIEW | SEQUENCE … OWNER TO role`.
    OwnerTo { relation: String, owner: String },
}

fn ident(word: &str) -> String {
    let w = word.trim();
    if w.starts_with('"') && w.ends_with('"') && w.len() >= 2 {
        w[1..w.len() - 1].replace("\"\"", "\"")
    } else {
        w.to_ascii_lowercase()
    }
}

fn name_of(word: &str) -> String {
    ident(crate::core::sql_scan::last_name_segment(word.trim()))
}

/// The privileges a list names, `ALL [PRIVILEGES]` as every one.
fn privilege_list(text: &str) -> Result<Vec<String>, Refusal> {
    let upper = text.trim().to_ascii_uppercase();
    if upper == "ALL" || upper == "ALL PRIVILEGES" {
        return Ok(TABLE_PRIVILEGES.iter().map(|p| p.to_string()).collect());
    }
    let mut out = Vec::new();
    for piece in crate::core::sql_scan::split_top_level(text, b',') {
        let p = piece.trim().to_ascii_uppercase();
        if p.contains('(') {
            return Err(("0A000", "column privileges are not supported yet".to_string()));
        }
        out.push(p);
    }
    Ok(out)
}

fn grantee_list(text: &str) -> Vec<String> {
    crate::core::sql_scan::split_top_level(text, b',')
        .into_iter()
        .map(|g| {
            let g = g.trim();
            let g = g.strip_prefix("GROUP ").or_else(|| g.strip_prefix("group ")).unwrap_or(g);
            ident(g)
        })
        .filter(|g| !g.is_empty())
        .collect()
}

/// Where a top-level keyword starts.
fn kw(text: &str, word: &str) -> Option<usize> {
    crate::core::rql::join::find_keyword(text, word, 0)
}

/// The target after `ON`, up to `end`.
fn target_of(text: &str) -> Target {
    let t = text.trim();
    let upper = t.to_ascii_uppercase();
    if upper.starts_with("ALL TABLES IN SCHEMA") {
        return Target::AllTablesInSchema;
    }
    for kind in ["SEQUENCE", "ALL SEQUENCES", "FUNCTION", "ALL FUNCTIONS", "PROCEDURE", "ALL PROCEDURES", "ROUTINE", "SCHEMA", "DATABASE", "LANGUAGE", "TYPE", "DOMAIN", "FOREIGN", "LARGE OBJECT", "TABLESPACE", "PARAMETER"] {
        if upper.starts_with(kind) {
            return Target::Other(kind.to_string());
        }
    }
    let list = if upper.starts_with("TABLE ") { &t[6..] } else { t };
    Target::Tables(crate::core::sql_scan::split_top_level(list, b',').into_iter().map(name_of).collect())
}

/// Parses `GRANT`, `REVOKE`, `ALTER DEFAULT PRIVILEGES`, and `ALTER TABLE
/// … OWNER TO …`; `None` for any other statement.
pub fn parse_privilege_statement(sql: &str) -> Option<Result<PrivilegeStatement, Refusal>> {
    let text = sql.trim().trim_end_matches(';').trim_end();
    let upper = text.to_ascii_uppercase();
    let syntax = |what: &str| ("42601".to_string(), what.to_string());
    let _ = syntax;
    if upper.starts_with("GRANT ") {
        let body = &text[6..];
        let Some(to) = kw(body, "TO") else {
            return Some(Err(("42601", "syntax error in GRANT: expected TO".to_string())));
        };
        let mut grantees_text = body[to + 2..].trim().to_string();
        let mut grantable = false;
        for suffix in ["WITH GRANT OPTION", "WITH ADMIN OPTION", "WITH INHERIT TRUE", "WITH SET TRUE"] {
            if let Some(at) = kw(&grantees_text, &suffix[..4]).filter(|at| grantees_text[*at..].to_ascii_uppercase().starts_with(suffix)) {
                grantable |= suffix == "WITH GRANT OPTION";
                grantees_text = grantees_text[..at].trim().to_string();
            }
        }
        if let Some(at) = kw(&grantees_text, "GRANTED") {
            grantees_text = grantees_text[..at].trim().to_string();
        }
        let grantees = grantee_list(&grantees_text);
        return Some(match kw(&body[..to], "ON") {
            Some(on) => privilege_list(&body[..on]).map(|privileges| PrivilegeStatement::Grant {
                privileges,
                target: target_of(&body[on + 2..to]),
                grantees,
                grantable,
            }),
            None => Ok(PrivilegeStatement::GrantRole { roles: grantee_list(&body[..to]), members: grantees }),
        });
    }
    if upper.starts_with("REVOKE ") {
        let mut body = &text[7..];
        let mut grant_option_only = false;
        if body.to_ascii_uppercase().starts_with("GRANT OPTION FOR ") {
            grant_option_only = true;
            body = &body[17..];
        } else if body.to_ascii_uppercase().starts_with("ADMIN OPTION FOR ") {
            body = &body[17..];
        }
        let Some(from) = kw(body, "FROM") else {
            return Some(Err(("42601", "syntax error in REVOKE: expected FROM".to_string())));
        };
        let mut grantees_text = body[from + 4..].trim().to_string();
        for suffix in ["CASCADE", "RESTRICT"] {
            if grantees_text.to_ascii_uppercase().ends_with(suffix) {
                grantees_text.truncate(grantees_text.len() - suffix.len());
            }
        }
        if let Some(at) = kw(&grantees_text, "GRANTED") {
            grantees_text = grantees_text[..at].trim().to_string();
        }
        let grantees = grantee_list(&grantees_text);
        return Some(match kw(&body[..from], "ON") {
            Some(on) => privilege_list(&body[..on]).map(|privileges| PrivilegeStatement::Revoke {
                privileges,
                target: target_of(&body[on + 2..from]),
                grantees,
                grant_option_only,
            }),
            None => Ok(PrivilegeStatement::RevokeRole { roles: grantee_list(&body[..from]), members: grantees }),
        });
    }
    if upper.starts_with("ALTER DEFAULT PRIVILEGES") {
        let mut rest = text[24..].trim();
        let mut for_role = None;
        loop {
            let u = rest.to_ascii_uppercase();
            if u.starts_with("FOR ROLE ") || u.starts_with("FOR USER ") {
                let after = rest[9..].trim_start();
                let end = after.find(char::is_whitespace).unwrap_or(after.len());
                for_role = Some(ident(after[..end].trim_end_matches(',')));
                rest = after[end..].trim_start();
            } else if u.starts_with("IN SCHEMA ") {
                let after = rest[10..].trim_start();
                let end = after.find(char::is_whitespace).unwrap_or(after.len());
                rest = after[end..].trim_start();
            } else {
                break;
            }
        }
        let u = rest.to_ascii_uppercase();
        let grant = u.starts_with("GRANT ");
        if !grant && !u.starts_with("REVOKE ") {
            return Some(Err(("42601", "syntax error in ALTER DEFAULT PRIVILEGES".to_string())));
        }
        let body = if grant { &rest[6..] } else { &rest[7..] };
        let (on, sep) = match (kw(body, "ON"), kw(body, if grant { "TO" } else { "FROM" })) {
            (Some(on), Some(sep)) if on < sep => (on, sep),
            _ => return Some(Err(("42601", "syntax error in ALTER DEFAULT PRIVILEGES".to_string()))),
        };
        let objects = body[on + 2..sep].trim().to_ascii_uppercase();
        let tables = objects == "TABLES";
        let mut grantees_text = body[sep + if grant { 2 } else { 4 }..].trim().to_string();
        for suffix in ["WITH GRANT OPTION", "CASCADE", "RESTRICT"] {
            if let Some(at) = grantees_text.to_ascii_uppercase().find(suffix) {
                grantees_text.truncate(at);
            }
        }
        let privileges = if tables {
            match privilege_list(&body[..on]) {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            }
        } else {
            Vec::new()
        };
        return Some(Ok(PrivilegeStatement::DefaultPrivileges {
            for_role,
            grant,
            privileges,
            grantees: grantee_list(&grantees_text),
            tables,
        }));
    }
    if upper.starts_with("ALTER ") {
        // `ALTER TABLE|VIEW|MATERIALIZED VIEW|SEQUENCE name OWNER TO role`.
        let owner_at = upper.find(" OWNER TO ")?;
        let head = text[..owner_at].split_whitespace().collect::<Vec<_>>();
        let relation = name_of(head.last()?);
        let owner = ident(text[owner_at + 10..].trim());
        return Some(Ok(PrivilegeStatement::OwnerTo { relation, owner }));
    }
    None
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

fn run(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let database = database.clone();
    let sql = sql.to_string();
    Box::pin(async move {
        let purpose = recognize_query_purpose(&sql).ok_or_else(|| format!("could not parse: {sql}"))?;
        let response = match tx {
            Some(tx) => executor::execute_in_txn(purpose, database, tx).await,
            None => tokio::spawn(executor::execute_as(crate::core::mvcc::owner::OwnerKind::Statement, purpose, database))
                .await
                .map_err(|e| format!("the privilege catalogue statement did not finish: {e}"))?,
        };
        if response.first() == Some(&3) {
            return Err(String::from_utf8_lossy(&response[1..]).to_string());
        }
        Ok(response)
    })
}

fn id_of(row: &AclRow) -> String {
    format!("{}\u{1}{}\u{1}{}", row.relation, row.grantee, row.privilege)
}

/// Every row `tx` sees.
pub async fn load(database: &Arc<Database>, tx: Option<TxId>) -> Result<Vec<AclRow>, String> {
    if !database.has_table(ACL_TABLE) {
        return Ok(Vec::new());
    }
    let response = run(database, &format!("SELECT relation, grantee, privilege, grantor, grantable FROM {ACL_TABLE}"), tx).await?;
    if response.first() != Some(&2) {
        return Ok(Vec::new());
    }
    let rows = crate::core::row::row::vec_into_rows(&response[1..]).map_err(|e| e.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let text = |i: usize| {
                row.columns.get(i).map(|c| String::from_utf8_lossy(c.data.into_slice()).into_owned()).unwrap_or_default()
            };
            AclRow {
                relation: text(0),
                grantee: text(1),
                privilege: text(2),
                grantor: text(3),
                grantable: row.columns.get(4).is_some_and(|c| c.data.into_slice().first().copied().unwrap_or(0) != 0),
            }
        })
        .collect())
}

/// Writes `row`, replacing one with its relation, grantee and privilege.
pub async fn put(database: &Arc<Database>, row: &AclRow, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::quote;
    if !database.has_table(ACL_TABLE) {
        run(database, ACL_TABLE_DDL, None).await?;
    }
    run(database, &format!("DELETE FROM {ACL_TABLE} WHERE id = {}", quote(&id_of(row))), tx).await?;
    let sql = format!(
        "INSERT INTO {ACL_TABLE} (id, relation, grantee, privilege, grantor, grantable) VALUES ({}, {}, {}, {}, {}, {})",
        quote(&id_of(row)),
        quote(&row.relation),
        quote(&row.grantee),
        quote(&row.privilege),
        quote(&row.grantor),
        row.grantable
    );
    run(database, &sql, tx).await.map(|_| ())
}

/// Deletes the rows `filter` (an SQL condition over the table) picks.
pub async fn delete_where(database: &Arc<Database>, filter: &str, tx: Option<TxId>) -> Result<(), String> {
    if !database.has_table(ACL_TABLE) {
        return Ok(());
    }
    run(database, &format!("DELETE FROM {ACL_TABLE} WHERE {filter}"), tx).await.map(|_| ())
}

/// Every row of `relation`, renamed to `to` (a renamed table keeps its
/// owner and grants).
pub async fn rename(database: &Arc<Database>, relation: &str, to: &str, tx: Option<TxId>) -> Result<(), String> {
    let rows: Vec<AclRow> = load(database, tx).await?.into_iter().filter(|r| r.relation == relation).collect();
    if rows.is_empty() {
        return Ok(());
    }
    delete_where(database, &format!("relation = {}", crate::auth::quote(relation)), tx).await?;
    for mut row in rows {
        row.relation = to.to_string();
        put(database, &row, tx).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Checking
// ---------------------------------------------------------------------------

/// The roles `role` acts as: itself, and every role it is a member of,
/// through memberships of memberships.
pub fn roles_of(rows: &[AclRow], role: &str) -> Vec<String> {
    let mut out = vec![role.to_string()];
    let mut at = 0;
    while at < out.len() {
        let member = out[at].clone();
        for row in rows.iter().filter(|r| r.privilege == "MEMBER" && r.grantee == member) {
            if let Some(granted) = row.relation.strip_prefix("@role:") {
                if !out.iter().any(|o| o == granted) {
                    out.push(granted.to_string());
                }
            }
        }
        at += 1;
    }
    out
}

/// `relation`'s owner, `None` for one created before owners were recorded.
pub fn owner_of<'a>(rows: &'a [AclRow], relation: &str) -> Option<&'a str> {
    rows.iter().find(|r| r.privilege == "OWNER" && r.relation == relation).map(|r| r.grantee.as_str())
}

/// Whether `role` may use `privilege` on `relation` (its owner may do all).
pub fn allows(rows: &[AclRow], role: &str, relation: &str, privilege: &str) -> bool {
    let Some(owner) = owner_of(rows, relation) else {
        return true;
    };
    let acting = roles_of(rows, role);
    if acting.iter().any(|r| r == owner) {
        return true;
    }
    rows.iter().any(|r| {
        r.relation == relation
            && r.privilege == privilege
            && (r.grantee == "public" || acting.iter().any(|a| *a == r.grantee))
    })
}

/// Whether `role` may grant `privilege` on `relation`: its owner, or a
/// holder of it `WITH GRANT OPTION`.
pub fn may_grant(rows: &[AclRow], role: &str, relation: &str, privilege: &str) -> bool {
    let acting = roles_of(rows, role);
    match owner_of(rows, relation) {
        None => true,
        Some(owner) if acting.iter().any(|r| r == owner) => true,
        Some(_) => rows.iter().any(|r| {
            r.relation == relation && r.privilege == privilege && r.grantable && acting.iter().any(|a| *a == r.grantee)
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privilege_statements_are_read() {
        assert_eq!(
            parse_privilege_statement("GRANT SELECT, INSERT ON TABLE public.a, b TO app, PUBLIC WITH GRANT OPTION").unwrap().unwrap(),
            PrivilegeStatement::Grant {
                privileges: vec!["SELECT".into(), "INSERT".into()],
                target: Target::Tables(vec!["a".into(), "b".into()]),
                grantees: vec!["app".into(), "public".into()],
                grantable: true,
            }
        );
        assert_eq!(
            parse_privilege_statement("REVOKE ALL PRIVILEGES ON t FROM app CASCADE").unwrap().unwrap(),
            PrivilegeStatement::Revoke {
                privileges: TABLE_PRIVILEGES.iter().map(|p| p.to_string()).collect(),
                target: Target::Tables(vec!["t".into()]),
                grantees: vec!["app".into()],
                grant_option_only: false,
            }
        );
        assert_eq!(
            parse_privilege_statement("GRANT readers TO alice, bob").unwrap().unwrap(),
            PrivilegeStatement::GrantRole { roles: vec!["readers".into()], members: vec!["alice".into(), "bob".into()] }
        );
        assert_eq!(
            parse_privilege_statement("ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA public GRANT SELECT ON TABLES TO app").unwrap().unwrap(),
            PrivilegeStatement::DefaultPrivileges {
                for_role: Some("owner".into()),
                grant: true,
                privileges: vec!["SELECT".into()],
                grantees: vec!["app".into()],
                tables: true,
            }
        );
        assert_eq!(
            parse_privilege_statement("GRANT USAGE ON SCHEMA public TO app").unwrap().unwrap(),
            PrivilegeStatement::Grant { privileges: vec!["USAGE".into()], target: Target::Other("SCHEMA".into()), grantees: vec!["app".into()], grantable: false }
        );
        assert_eq!(
            parse_privilege_statement("ALTER TABLE t OWNER TO app").unwrap().unwrap(),
            PrivilegeStatement::OwnerTo { relation: "t".into(), owner: "app".into() }
        );
        assert!(parse_privilege_statement("ALTER TABLE t ADD COLUMN x INT").is_none());
        assert!(parse_privilege_statement("GRANT SELECT (a) ON t TO app").unwrap().is_err());
    }

    #[test]
    fn owners_grants_and_memberships_decide() {
        let row = |relation: &str, grantee: &str, privilege: &str| AclRow {
            relation: relation.into(),
            grantee: grantee.into(),
            privilege: privilege.into(),
            grantor: "o".into(),
            grantable: false,
        };
        let rows = vec![
            row("t", "o", "OWNER"),
            row("t", "readers", "SELECT"),
            row("@role:readers", "alice", "MEMBER"),
            row("u", "o", "OWNER"),
            row("u", "public", "INSERT"),
        ];
        assert!(allows(&rows, "o", "t", "DELETE"));
        assert!(allows(&rows, "alice", "t", "SELECT"));
        assert!(!allows(&rows, "alice", "t", "INSERT"));
        assert!(!allows(&rows, "bob", "t", "SELECT"));
        assert!(allows(&rows, "bob", "u", "INSERT"));
        assert!(allows(&rows, "bob", "legacy", "DELETE"));
        assert!(!may_grant(&rows, "alice", "t", "SELECT"));
    }
}
