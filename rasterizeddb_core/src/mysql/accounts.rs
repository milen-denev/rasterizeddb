//! MySQL's account statements as the engine's role statements (roles plan
//! R2): `CREATE USER 'u'@'%' IDENTIFIED BY 'p'`, `ALTER USER`, `DROP USER`,
//! `CREATE ROLE`/`DROP ROLE`, `SET PASSWORD [FOR u] = 'p'`, and `GRANT` /
//! `REVOKE` of privileges (`ON db.t`, `db.*`, `*.*`) or of roles.
//!
//! A host part (`@'%'`, `@localhost`) is read and dropped: roles here are not
//! told apart by where a client connects from. A database other than the
//! session's is refused rather than read as this one, and a privilege with
//! no counterpart (`CREATE`, `DROP`, `ALTER`, `INDEX`, `FILE`, …) is refused
//! by name rather than granted as nothing.

use super::lex::{Tok, Token};
use super::translate::MyError;

/// Where the session's own name goes in a statement made for `SET PASSWORD`
/// without `FOR`: filled in by the session, which knows it.
pub(crate) const SESSION_USER: &str = "\u{0}session-user\u{0}";

fn upper(tokens: &[Token], i: usize) -> String {
    tokens.get(i).and_then(|t| t.upper()).unwrap_or_default()
}

/// An identifier in double quotes, as the engine reads a name whose case
/// counts: MySQL's account names are case-sensitive.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// An account (`'u'@'h'`, `u@h`, `` `u` ``, `u`) at `i`: its name, and the
/// index after it.
fn account(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    let name = match &tokens.get(i)?.tok {
        Tok::Str(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Tok::Word(w) | Tok::Quoted(w) => w.clone(),
        _ => return None,
    };
    let mut next = i + 1;
    if matches!(tokens.get(next).map(|t| &t.tok), Some(Tok::UserVar(_))) {
        next += 1;
    }
    Some((name, next))
}

/// A comma list of accounts from `i`, up to a word in `stop` or the end.
fn accounts(tokens: &[Token], mut i: usize, stop: &[&str]) -> Result<(Vec<String>, usize), MyError> {
    let mut out = Vec::new();
    loop {
        let Some((name, next)) = account(tokens, i) else {
            return Err(MyError::syntax("an account name was expected"));
        };
        out.push(name);
        i = next;
        if tokens.get(i).is_some_and(|t| t.is_op(",")) {
            i += 1;
            continue;
        }
        if i < tokens.len() && !stop.iter().any(|s| upper(tokens, i) == *s) {
            return Err(MyError::syntax(&format!("unexpected {:?} after the account list", tokens[i].tok)));
        }
        return Ok((out, i));
    }
}

/// `IDENTIFIED [WITH plugin] BY 'p'` at `i`: the password, and the index
/// after it. `None` when there is none at `i`.
fn identified_by(tokens: &[Token], i: usize) -> Result<Option<(String, usize)>, MyError> {
    if upper(tokens, i) != "IDENTIFIED" {
        return Ok(None);
    }
    let mut at = i + 1;
    if upper(tokens, at) == "WITH" {
        // The plugin is MySQL's; a password here becomes this server's
        // verifier whatever plugin was named.
        at += 2;
    }
    if upper(tokens, at) != "BY" {
        return Err(MyError::unsupported("IDENTIFIED without BY 'password'"));
    }
    match tokens.get(at + 1).map(|t| &t.tok) {
        Some(Tok::Str(p)) => Ok(Some((String::from_utf8_lossy(p).into_owned(), at + 2))),
        _ => Err(MyError::syntax("IDENTIFIED BY takes a quoted password")),
    }
}

/// MySQL's privilege names as the engine's, `Ok(None)` for `USAGE` (no
/// privilege at all, which MySQL grants to say so).
fn privilege(name: &str) -> Result<Option<&'static str>, MyError> {
    Ok(Some(match name {
        "SELECT" => "SELECT",
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "REFERENCES" => "REFERENCES",
        "TRIGGER" => "TRIGGER",
        "EXECUTE" => "EXECUTE",
        "USAGE" => return Ok(None),
        other => {
            return Err(MyError::unsupported(&format!(
                "the {other} privilege: grant SELECT, INSERT, UPDATE, DELETE, REFERENCES, TRIGGER or EXECUTE; \
                 schema changes are the owner's"
            )));
        }
    }))
}

/// `GRANT …` or `REVOKE …` (`grant` false). `database` is the session's.
fn grant_or_revoke(tokens: &[Token], grant: bool, database: &str) -> Result<Vec<String>, MyError> {
    let verb = if grant { "GRANT" } else { "REVOKE" };
    let to = if grant { "TO" } else { "FROM" };
    let Some(on) = (1..tokens.len()).find(|&i| upper(tokens, i) == "ON" || upper(tokens, i) == to) else {
        return Err(MyError::syntax(&format!("{verb} needs {to}")));
    };
    // `GRANT r1, r2 TO u`: roles, not privileges.
    if upper(tokens, on) == to {
        let (roles, _) = accounts(tokens, 1, &[to])?;
        let (members, end) = accounts(tokens, on + 1, &["WITH"])?;
        let admin = upper(tokens, end) == "WITH" && upper(tokens, end + 1) == "ADMIN";
        let roles: Vec<String> = roles.iter().map(|r| quote_ident(r)).collect();
        let members: Vec<String> = members.iter().map(|m| quote_ident(m)).collect();
        return Ok(vec![format!(
            "{verb} {} {to} {}{}",
            roles.join(", "),
            members.join(", "),
            if admin { " WITH ADMIN OPTION" } else { "" }
        )]);
    }
    // The privileges, before ON: `ALL [PRIVILEGES]` or a list. A column
    // list (`SELECT (a, b)`) is not modelled.
    let mut privileges: Vec<&'static str> = Vec::new();
    let mut all = false;
    let mut i = 1;
    while i < on {
        let word = upper(tokens, i);
        if tokens[i].is_op(",") {
            i += 1;
            continue;
        }
        if tokens[i].is_op("(") {
            return Err(MyError::unsupported("column privileges"));
        }
        match word.as_str() {
            "ALL" => {
                all = true;
                if upper(tokens, i + 1) == "PRIVILEGES" {
                    i += 1;
                }
            }
            "GRANT" if upper(tokens, i + 1) == "OPTION" => {
                return Err(MyError::unsupported("GRANT OPTION as a privilege (use WITH GRANT OPTION)"));
            }
            _ => {
                if let Some(p) = privilege(&word)? {
                    privileges.push(p);
                }
            }
        }
        i += 1;
    }
    // ON [TABLE|FUNCTION|PROCEDURE] {*.* | db.* | * | db.t | t}
    let mut at = on + 1;
    let kind = match upper(tokens, at).as_str() {
        "TABLE" => {
            at += 1;
            "TABLE"
        }
        "FUNCTION" | "PROCEDURE" => {
            let k = if upper(tokens, at) == "FUNCTION" { "FUNCTION" } else { "PROCEDURE" };
            at += 1;
            k
        }
        _ => "TABLE",
    };
    let star = |i: usize| tokens.get(i).is_some_and(|t| t.is_op("*"));
    let name_at = |i: usize| tokens.get(i).and_then(|t| t.ident()).map(str::to_string);
    let (schema, object, after) = if star(at) && tokens.get(at + 1).is_some_and(|t| t.is_op(".")) && star(at + 2) {
        (None, None, at + 3)
    } else if star(at) {
        (None, None, at + 1)
    } else {
        let Some(first) = name_at(at) else {
            return Err(MyError::syntax(&format!("{verb} … ON needs an object")));
        };
        if tokens.get(at + 1).is_some_and(|t| t.is_op(".")) {
            if star(at + 2) {
                (Some(first), None, at + 3)
            } else {
                let Some(second) = name_at(at + 2) else {
                    return Err(MyError::syntax(&format!("{verb} … ON db.object needs an object")));
                };
                (Some(first), Some(second), at + 3)
            }
        } else {
            (None, Some(first), at + 1)
        }
    };
    if let Some(schema) = &schema
        && schema != database
    {
        return Err(MyError::unsupported(&format!(
            "{verb} on database `{schema}` from a session in `{database}`: connect to `{schema}` and {verb} there"
        )));
    }
    if upper(tokens, after) != to {
        return Err(MyError::syntax(&format!("{verb} needs {to} after its object")));
    }
    let (grantees, end) = accounts(tokens, after + 1, &["WITH", "IDENTIFIED", "REQUIRE"])?;
    let grant_option = grant && upper(tokens, end) == "WITH" && upper(tokens, end + 1) == "GRANT";
    if all {
        privileges = match kind {
            "TABLE" => vec!["ALL"],
            _ => vec!["EXECUTE"],
        };
    }
    if privileges.is_empty() {
        // `GRANT USAGE ON *.* TO u`: MySQL's way of saying nothing.
        return Ok(Vec::new());
    }
    let target = match (kind, object) {
        ("TABLE", None) => "ALL TABLES IN SCHEMA public".to_string(),
        ("TABLE", Some(t)) => format!("TABLE {}", quote_ident(&t)),
        (k, Some(f)) => format!("{k} {}", quote_ident(&f)),
        (_, None) => "ALL FUNCTIONS IN SCHEMA public".to_string(),
    };
    let grantees: Vec<String> = grantees.iter().map(|g| quote_ident(g)).collect();
    Ok(vec![format!(
        "{verb} {} ON {target} {to} {}{}",
        privileges.join(", "),
        grantees.join(", "),
        if grant_option { " WITH GRANT OPTION" } else { "" }
    )])
}

/// The engine's statements for MySQL account statement `tokens`, or `None`
/// when `tokens` is no such statement. `database` is the session's.
pub(crate) fn translate(tokens: &[Token], database: &str) -> Option<Result<Vec<String>, MyError>> {
    let head = upper(tokens, 0);
    let second = upper(tokens, 1);
    // The role statements also come in PostgreSQL's own form through this
    // front end (`CREATE ROLE r LOGIN`, `ALTER USER u PASSWORD 'x'`), which
    // the engine reads as they are: translated only in MySQL's.
    let mysql_form = tokens.iter().any(|t| matches!(t.tok, Tok::UserVar(_)) || t.is_word("IDENTIFIED") || t.is_word("ACCOUNT"))
        || tokens.get(2..5).is_some_and(|w| w.iter().any(|t| matches!(t.tok, Tok::Str(_))));
    if matches!(head.as_str(), "CREATE" | "ALTER" | "DROP") && matches!(second.as_str(), "USER" | "ROLE") && !mysql_form {
        return None;
    }
    Some(match (head.as_str(), second.as_str()) {
        ("CREATE", "USER") | ("CREATE", "ROLE") => {
            let mut at = 2;
            let if_not_exists = upper(tokens, 2) == "IF";
            if if_not_exists {
                at += 3;
            }
            (|| {
                let mut out = Vec::new();
                loop {
                    let Some((name, next)) = account(tokens, at) else {
                        return Err(MyError::syntax("CREATE USER names an account"));
                    };
                    at = next;
                    let password = match identified_by(tokens, at)? {
                        Some((p, next)) => {
                            at = next;
                            Some(p)
                        }
                        None => None,
                    };
                    out.push(format!(
                        "CREATE ROLE {}{}{}",
                        quote_ident(&name),
                        if second == "USER" { " LOGIN" } else { "" },
                        password.map(|p| format!(" PASSWORD {}", quote_literal(&p))).unwrap_or_default()
                    ));
                    if tokens.get(at).is_some_and(|t| t.is_op(",")) {
                        at += 1;
                        continue;
                    }
                    // DEFAULT ROLE, REQUIRE, WITH resource limits, PASSWORD
                    // EXPIRE, ACCOUNT LOCK: MySQL's; none is kept here.
                    if at < tokens.len() {
                        return Err(MyError::unsupported(&format!(
                            "{} in CREATE USER (only IDENTIFIED BY is kept)",
                            upper(tokens, at)
                        )));
                    }
                    return Ok(out);
                }
            })()
        }
        ("ALTER", "USER") => (|| {
            let mut at = 2;
            if upper(tokens, 2) == "IF" {
                at += 2;
            }
            let Some((name, next)) = account(tokens, at) else {
                return Err(MyError::syntax("ALTER USER names an account"));
            };
            let name = if upper(tokens, at) == "USER" && tokens.get(at + 1).is_some_and(|t| t.is_op("(")) {
                // `ALTER USER USER() IDENTIFIED BY …`: oneself.
                at += 3;
                SESSION_USER.to_string()
            } else {
                at = next;
                name
            };
            match identified_by(tokens, at)? {
                Some((p, end)) if end == tokens.len() => {
                    let who = if name == SESSION_USER { name } else { quote_ident(&name) };
                    Ok(vec![format!("ALTER ROLE {who} PASSWORD {}", quote_literal(&p))])
                }
                Some(_) => Err(MyError::unsupported("ALTER USER options besides IDENTIFIED BY")),
                None if upper(tokens, at) == "ACCOUNT" => {
                    let lock = upper(tokens, at + 1) == "LOCK";
                    Ok(vec![format!("ALTER ROLE {} {}", quote_ident(&name), if lock { "NOLOGIN" } else { "LOGIN" })])
                }
                None => Err(MyError::unsupported("ALTER USER options besides IDENTIFIED BY and ACCOUNT LOCK/UNLOCK")),
            }
        })(),
        ("RENAME", "USER") => (|| {
            let Some((from, next)) = account(tokens, 2) else {
                return Err(MyError::syntax("RENAME USER names an account"));
            };
            if upper(tokens, next) != "TO" {
                return Err(MyError::syntax("RENAME USER a TO b"));
            }
            let Some((to, end)) = account(tokens, next + 1) else {
                return Err(MyError::syntax("RENAME USER a TO b"));
            };
            if end != tokens.len() {
                return Err(MyError::unsupported("RENAME USER of more than one account at once"));
            }
            Ok(vec![format!("ALTER ROLE {} RENAME TO {}", quote_ident(&from), quote_ident(&to))])
        })(),
        ("DROP", "USER") | ("DROP", "ROLE") => (|| {
            let mut at = 2;
            let if_exists = upper(tokens, 2) == "IF";
            if if_exists {
                at += 2;
            }
            let (names, end) = accounts(tokens, at, &[])?;
            if end != tokens.len() {
                return Err(MyError::syntax("DROP USER takes accounts only"));
            }
            Ok(names
                .iter()
                .map(|n| format!("DROP ROLE {}{}", if if_exists { "IF EXISTS " } else { "" }, quote_ident(n)))
                .collect())
        })(),
        ("SET", "PASSWORD") => (|| {
            let (who, at) = if upper(tokens, 2) == "FOR" {
                let Some((name, next)) = account(tokens, 3) else {
                    return Err(MyError::syntax("SET PASSWORD FOR names an account"));
                };
                (quote_ident(&name), next)
            } else {
                (SESSION_USER.to_string(), 2)
            };
            if !tokens.get(at).is_some_and(|t| t.is_op("=")) {
                return Err(MyError::syntax("SET PASSWORD … = 'password'"));
            }
            match tokens.get(at + 1).map(|t| &t.tok) {
                Some(Tok::Str(p)) if at + 2 == tokens.len() => {
                    Ok(vec![format!("ALTER ROLE {who} PASSWORD {}", quote_literal(&String::from_utf8_lossy(p)))])
                }
                _ => Err(MyError::unsupported("SET PASSWORD other than = 'password'")),
            }
        })(),
        ("GRANT", _) => grant_or_revoke(tokens, true, database),
        ("REVOKE", _) => grant_or_revoke(tokens, false, database),
        _ => return None,
    })
}
