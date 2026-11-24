//! T-SQL's logins, users and role membership as the engine's role
//! statements (roles plan R2): `CREATE LOGIN l WITH PASSWORD = 'p'`, `ALTER
//! LOGIN … WITH PASSWORD | DISABLE | ENABLE | NAME`, `DROP LOGIN`, `CREATE
//! USER u [FOR|FROM LOGIN u] | WITHOUT LOGIN`, `DROP USER`, `CREATE ROLE`,
//! `ALTER ROLE r ADD|DROP MEMBER u`, and `GRANT`/`REVOKE` on an object.
//!
//! A login and its database user are one role here: `CREATE USER u FOR
//! LOGIN u` names what exists and does nothing, and one for a login of
//! another name is refused rather than made a second role. `DENY` and the
//! fixed database roles (`db_datareader`, …) have no counterpart and are
//! refused by name: granting a snapshot of today's tables in their place
//! would not cover tomorrow's.

use super::translate::TsqlError;
use crate::sqlcompat::{Tok, Token};

fn upper(tokens: &[Token], i: usize) -> String {
    tokens.get(i).and_then(|t| t.upper()).unwrap_or_default()
}

fn name(tokens: &[Token], i: usize) -> Option<String> {
    let t = tokens.get(i)?;
    match &t.tok {
        Tok::Word(_) | Tok::Quoted(_) => t.ident().map(str::to_string),
        _ => None,
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `[dbo.]name` from `i`: the last part, and the index after it.
fn object(tokens: &[Token], mut i: usize) -> Option<(String, usize)> {
    let mut last = name(tokens, i)?;
    i += 1;
    while tokens.get(i).is_some_and(|t| t.is_op(".")) {
        last = name(tokens, i + 1)?;
        i += 2;
    }
    Some((last, i))
}

/// `PASSWORD = 'p'` at `i`: the password and the index after it.
fn password_at(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    if upper(tokens, i) != "PASSWORD" || !tokens.get(i + 1).is_some_and(|t| t.is_op("=")) {
        return None;
    }
    match tokens.get(i + 2).map(|t| &t.tok) {
        Some(Tok::Str(p)) => Some((String::from_utf8_lossy(p).into_owned(), i + 3)),
        _ => None,
    }
}

const FIXED_ROLES: &[&str] = &[
    "db_owner",
    "db_datareader",
    "db_datawriter",
    "db_ddladmin",
    "db_securityadmin",
    "db_accessadmin",
    "db_backupoperator",
    "db_denydatareader",
    "db_denydatawriter",
    "sysadmin",
    "securityadmin",
    "serveradmin",
];

fn fixed_role_refusal(role: &str) -> TsqlError {
    TsqlError::unsupported(format!(
        "the fixed role {role}: grant the privileges themselves (GRANT SELECT ON t TO u), or make a role of your own"
    ))
}

/// `GRANT`/`REVOKE p[, …] ON [OBJECT::]obj TO|FROM u[, …] [WITH GRANT
/// OPTION] [CASCADE]`.
fn grant_or_revoke(tokens: &[Token], grant: bool) -> Result<Vec<String>, TsqlError> {
    let verb = if grant { "GRANT" } else { "REVOKE" };
    let to = if grant { "TO" } else { "FROM" };
    let Some(on) = (1..tokens.len()).find(|&i| upper(tokens, i) == "ON") else {
        return Err(TsqlError::unsupported(format!(
            "{verb} without ON: database permissions (CREATE TABLE, CONNECT, …) are not modelled"
        )));
    };
    let mut privileges = Vec::new();
    for t in &tokens[1..on] {
        if t.is_op(",") {
            continue;
        }
        let p = t.upper().unwrap_or_default();
        match p.as_str() {
            "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "REFERENCES" | "EXECUTE" => privileges.push(p),
            "ALL" | "PRIVILEGES" => privileges.push("ALL".to_string()),
            other => return Err(TsqlError::unsupported(format!("the {other} permission"))),
        }
    }
    privileges.dedup();
    let mut at = on + 1;
    // `OBJECT::dbo.t`, `SCHEMA::dbo`.
    let mut schema_wide = false;
    if tokens.get(at + 1).is_some_and(|t| t.is_op("::")) || tokens.get(at + 1).is_some_and(|t| t.is_op(":")) {
        let class = upper(tokens, at);
        at += if tokens.get(at + 1).is_some_and(|t| t.is_op("::")) { 2 } else { 3 };
        match class.as_str() {
            "OBJECT" => {}
            "SCHEMA" => schema_wide = true,
            other => return Err(TsqlError::unsupported(format!("{verb} on {other}::"))),
        }
    }
    let Some((target, after)) = object(tokens, at) else {
        return Err(TsqlError::new(102, format!("Incorrect syntax near {verb}: an object was expected after ON.")));
    };
    if upper(tokens, after) != to {
        return Err(TsqlError::new(102, format!("Incorrect syntax: {verb} … ON {target} needs {to}.")));
    }
    let mut grantees = Vec::new();
    let mut i = after + 1;
    while let Some(g) = name(tokens, i) {
        if FIXED_ROLES.contains(&g.to_ascii_lowercase().as_str()) {
            return Err(fixed_role_refusal(&g));
        }
        grantees.push(quote_ident(&g));
        i += 1;
        if tokens.get(i).is_some_and(|t| t.is_op(",")) {
            i += 1;
        } else {
            break;
        }
    }
    if grantees.is_empty() {
        return Err(TsqlError::new(102, format!("Incorrect syntax: {verb} names no one to {to}.")));
    }
    let with_grant = grant && upper(tokens, i) == "WITH" && upper(tokens, i + 1) == "GRANT";
    let cascade = !grant && upper(tokens, i) == "CASCADE";
    let executes = privileges.iter().all(|p| p == "EXECUTE");
    let on_text = if schema_wide {
        if executes { "ALL FUNCTIONS IN SCHEMA public".to_string() } else { "ALL TABLES IN SCHEMA public".to_string() }
    } else if executes {
        format!("FUNCTION {}", quote_ident(&target))
    } else {
        format!("TABLE {}", quote_ident(&target))
    };
    Ok(vec![format!(
        "{verb} {} ON {on_text} {to} {}{}{}",
        privileges.join(", "),
        grantees.join(", "),
        if with_grant { " WITH GRANT OPTION" } else { "" },
        if cascade { " CASCADE" } else { "" }
    )])
}

/// The engine's statements for a T-SQL login, user or permission statement,
/// or `None` when `tokens` is no such statement. An empty list: nothing to
/// do (a user for its own login).
pub(crate) fn translate(tokens: &[Token]) -> Option<Result<Vec<String>, TsqlError>> {
    let head = upper(tokens, 0);
    let second = upper(tokens, 1);
    Some(match (head.as_str(), second.as_str()) {
        ("CREATE", "LOGIN") => (|| {
            let login = name(tokens, 2).ok_or_else(|| TsqlError::new(102, "CREATE LOGIN names a login."))?;
            if upper(tokens, 3) != "WITH" {
                return Err(TsqlError::unsupported("CREATE LOGIN FROM WINDOWS, a certificate or a key"));
            }
            let Some((password, end)) = password_at(tokens, 4) else {
                return Err(TsqlError::new(102, "CREATE LOGIN … WITH PASSWORD = 'password'"));
            };
            // CHECK_POLICY, DEFAULT_DATABASE, …: SQL Server's own; kept nowhere.
            let _ = end;
            Ok(vec![format!("CREATE ROLE {} LOGIN PASSWORD {}", quote_ident(&login), quote_literal(&password))])
        })(),
        ("ALTER", "LOGIN") => (|| {
            let login = name(tokens, 2).ok_or_else(|| TsqlError::new(102, "ALTER LOGIN names a login."))?;
            match upper(tokens, 3).as_str() {
                "DISABLE" => Ok(vec![format!("ALTER ROLE {} NOLOGIN", quote_ident(&login))]),
                "ENABLE" => Ok(vec![format!("ALTER ROLE {} LOGIN", quote_ident(&login))]),
                "WITH" => {
                    if let Some((password, _)) = password_at(tokens, 4) {
                        return Ok(vec![format!("ALTER ROLE {} PASSWORD {}", quote_ident(&login), quote_literal(&password))]);
                    }
                    if upper(tokens, 4) == "NAME" && tokens.get(5).is_some_and(|t| t.is_op("=")) {
                        let to = name(tokens, 6).ok_or_else(|| TsqlError::new(102, "ALTER LOGIN … WITH NAME = new_name"))?;
                        return Ok(vec![format!("ALTER ROLE {} RENAME TO {}", quote_ident(&login), quote_ident(&to))]);
                    }
                    Err(TsqlError::unsupported("ALTER LOGIN options besides PASSWORD and NAME"))
                }
                _ => Err(TsqlError::unsupported("ALTER LOGIN other than WITH, ENABLE and DISABLE")),
            }
        })(),
        ("DROP", "LOGIN") => match name(tokens, 2) {
            Some(login) => Ok(vec![format!("DROP ROLE {}", quote_ident(&login))]),
            None => Err(TsqlError::new(102, "DROP LOGIN names a login.")),
        },
        ("CREATE", "USER") => (|| {
            let user = name(tokens, 2).ok_or_else(|| TsqlError::new(102, "CREATE USER names a user."))?;
            match (upper(tokens, 3).as_str(), upper(tokens, 4).as_str()) {
                // The login's own user: the login is the role already.
                ("FOR" | "FROM", "LOGIN") => {
                    let login = name(tokens, 5).ok_or_else(|| TsqlError::new(102, "CREATE USER … FOR LOGIN names a login."))?;
                    if login != user {
                        return Err(TsqlError::unsupported(format!(
                            "a user named apart from its login ({user} for {login}): a login and its user are one role here"
                        )));
                    }
                    Ok(Vec::new())
                }
                ("", _) => Ok(Vec::new()),
                ("WITHOUT", "LOGIN") => Ok(vec![format!("CREATE ROLE {}", quote_ident(&user))]),
                ("WITH", _) => {
                    // A contained user: `WITH PASSWORD = 'p'`.
                    match password_at(tokens, 4) {
                        Some((p, _)) => Ok(vec![format!("CREATE ROLE {} LOGIN PASSWORD {}", quote_ident(&user), quote_literal(&p))]),
                        None => Ok(Vec::new()),
                    }
                }
                _ => Err(TsqlError::unsupported("CREATE USER from a certificate, a key or an external provider")),
            }
        })(),
        // A user is its login's role: dropping the user leaves the login.
        ("DROP", "USER") => Ok(Vec::new()),
        ("CREATE", "ROLE") => match name(tokens, 2) {
            Some(role) if tokens.len() == 3 || upper(tokens, 3) == "AUTHORIZATION" => {
                Ok(vec![format!("CREATE ROLE {}", quote_ident(&role))])
            }
            _ => return None,
        },
        ("ALTER", "ROLE") if matches!(upper(tokens, 3).as_str(), "ADD" | "DROP") && upper(tokens, 4) == "MEMBER" => {
            (|| {
                let role = name(tokens, 2).ok_or_else(|| TsqlError::new(102, "ALTER ROLE names a role."))?;
                if FIXED_ROLES.contains(&role.to_ascii_lowercase().as_str()) {
                    return Err(fixed_role_refusal(&role));
                }
                let member = name(tokens, 5).ok_or_else(|| TsqlError::new(102, "ALTER ROLE … MEMBER names a member."))?;
                Ok(vec![if upper(tokens, 3) == "ADD" {
                    format!("GRANT {} TO {}", quote_ident(&role), quote_ident(&member))
                } else {
                    format!("REVOKE {} FROM {}", quote_ident(&role), quote_ident(&member))
                }])
            })()
        }
        ("GRANT", _) => grant_or_revoke(tokens, true),
        ("REVOKE", _) => grant_or_revoke(tokens, false),
        ("DENY", _) => Err(TsqlError::unsupported(
            "DENY: a permission here is granted or not, and no grant can be overridden; REVOKE the grant instead",
        )),
        _ => return None,
    })
}
