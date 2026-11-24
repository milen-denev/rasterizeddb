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
//! need a grant, as in PostgreSQL. Every check fails closed: a relation with
//! no owner row is its owner's alone, which nobody else is, so only a
//! superuser reaches it, and an ACL that cannot be read refuses. A database
//! from before owners existed is given them once, when its roles are
//! prepared ([`assign_owners`]): its owner owns every relation, and PUBLIC is
//! granted on each what every role had, so it keeps working. A superuser
//! passes every check; under `trust` a session is only what its role is
//! (`pgwire::authenticate`).

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
    /// `SEQUENCE a, b` (R2): USAGE, SELECT, UPDATE.
    Sequences(Vec<String>),
    AllSequencesInSchema,
    /// `FUNCTION f(…), PROCEDURE p, ROUTINE r` (R2): EXECUTE. `kind` is
    /// `function`, `procedure` or `routine` (either).
    Routines { kind: &'static str, names: Vec<String> },
    AllRoutinesInSchema { kind: &'static str },
    /// A schema, database or type: accepted, nothing enforced for it here.
    Other(String),
}

/// What kind of object a default privilege is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultKind {
    Tables,
    Sequences,
    Functions,
    Other,
}

impl DefaultKind {
    /// The ACL relation `role`'s defaults of this kind are kept under. Tables
    /// keep the name they always had.
    pub fn relation(self, role: &str) -> String {
        match self {
            DefaultKind::Tables | DefaultKind::Other => format!("@default:{role}"),
            DefaultKind::Sequences => format!("@default:{role}:sequences"),
            DefaultKind::Functions => format!("@default:{role}:functions"),
        }
    }

    /// The privileges an object of this kind takes.
    pub fn privileges(self) -> &'static [&'static str] {
        match self {
            DefaultKind::Tables | DefaultKind::Other => TABLE_PRIVILEGES,
            DefaultKind::Sequences => SEQUENCE_PRIVILEGES,
            DefaultKind::Functions => ROUTINE_PRIVILEGES,
        }
    }
}

/// Every ACL relation `role`'s default privileges are kept under.
pub fn default_relations(role: &str) -> [String; 3] {
    [DefaultKind::Tables.relation(role), DefaultKind::Sequences.relation(role), DefaultKind::Functions.relation(role)]
}

/// The privileges a sequence takes.
pub const SEQUENCE_PRIVILEGES: &[&str] = &["USAGE", "SELECT", "UPDATE"];
/// The privilege a function or procedure takes.
pub const ROUTINE_PRIVILEGES: &[&str] = &["EXECUTE"];
/// The marker row that PUBLIC may not execute a routine: PostgreSQL grants
/// PUBLIC EXECUTE on every function it creates, so the default is no row,
/// and routines made before privileges existed keep it.
pub const NOEXECUTE: &str = "NOEXECUTE";

/// `privs` for an object taking `valid`: `ALL` (read as every table
/// privilege) becomes `valid`; `Err` with the first one it does not take.
pub fn privileges_for(privs: Vec<String>, valid: &[&str]) -> Result<Vec<String>, String> {
    if privs.len() == TABLE_PRIVILEGES.len() && privs.iter().zip(TABLE_PRIVILEGES).all(|(a, b)| a == b) {
        return Ok(valid.iter().map(|p| p.to_string()).collect());
    }
    match privs.iter().find(|p| !valid.contains(&p.as_str())) {
        Some(p) => Err(p.clone()),
        None => Ok(privs),
    }
}

/// Whether `role` may execute the routine kept under `key`
/// (`@function:name`, `@procedure:name`): its owner may, PUBLIC may unless
/// a [`NOEXECUTE`] marker says otherwise, and a grantee of EXECUTE may.
pub fn may_execute(rows: &[AclRow], role: &str, key: &str) -> bool {
    let acting = roles_of(rows, role);
    if owner_of(rows, key).is_some_and(|o| acting.iter().any(|r| r == o)) {
        return true;
    }
    if !rows.iter().any(|r| r.relation == key && r.grantee == "public" && r.privilege == NOEXECUTE) {
        return true;
    }
    rows.iter().any(|r| r.relation == key && r.privilege == "EXECUTE" && acting.contains(&r.grantee))
}

/// One option of a role membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipOption {
    Admin,
    Inherit,
    Set,
}

/// The companion row that switches a membership's INHERIT off: its
/// privileges are not the member's, though SET ROLE may still reach it.
pub const NOINHERIT: &str = "NOINHERIT";
/// The companion row that switches a membership's SET off: SET ROLE may not
/// reach it, though its privileges may be inherited.
pub const NOSET: &str = "NOSET";

/// The `WITH …` options after a GRANT's grantees: (GRANT OPTION, ADMIN,
/// INHERIT, SET). PostgreSQL 16's forms and the older `WITH … OPTION`.
fn grant_with(text: &str) -> Result<(bool, bool, Option<bool>, Option<bool>), (&'static str, String)> {
    let (mut grant, mut admin, mut inherit, mut set) = (false, false, None, None);
    for part in text.split(',') {
        let words: Vec<String> = part.split_whitespace().map(str::to_ascii_uppercase).collect();
        let value = match words.get(1).map(String::as_str) {
            Some("OPTION" | "TRUE") if words.len() == 2 => true,
            Some("FALSE") if words.len() == 2 => false,
            _ => return Err(("42601", format!("syntax error in GRANT at WITH {}", part.trim()))),
        };
        match words[0].as_str() {
            "GRANT" => grant = value,
            "ADMIN" => admin = value,
            "INHERIT" => inherit = Some(value),
            "SET" => set = Some(value),
            other => return Err(("42601", format!("unrecognized role option \"{other}\""))),
        }
    }
    Ok((grant, admin, inherit, set))
}

/// A privilege statement, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivilegeStatement {
    Grant { privileges: Vec<String>, target: Target, grantees: Vec<String>, grantable: bool },
    Revoke { privileges: Vec<String>, target: Target, grantees: Vec<String>, grant_option_only: bool },
    /// `GRANT r TO m [WITH ADMIN|INHERIT|SET {OPTION|TRUE|FALSE}, …]`
    /// (R2): `admin` the ADMIN option; `inherit`/`set` `None` where unsaid.
    GrantRole { roles: Vec<String>, members: Vec<String>, admin: bool, inherit: Option<bool>, set: Option<bool> },
    /// `REVOKE [ADMIN|INHERIT|SET OPTION FOR] r FROM m`: `option` names the
    /// one option taken back, the membership kept; `None` revokes it.
    RevokeRole { roles: Vec<String>, members: Vec<String>, option: Option<MembershipOption> },
    /// `ALTER DEFAULT PRIVILEGES [FOR ROLE r] [IN SCHEMA s] GRANT|REVOKE … ON
    /// TABLES|SEQUENCES|FUNCTIONS|ROUTINES|TYPES|SCHEMAS …`; `for_role`
    /// `None` for the current role. `grant` false for REVOKE. `kind`
    /// [`DefaultKind::Other`] for types and schemas (accepted, unenforced).
    DefaultPrivileges { for_role: Option<String>, grant: bool, privileges: Vec<String>, grantees: Vec<String>, kind: DefaultKind },
    /// `ALTER TABLE | VIEW | SEQUENCE … OWNER TO role`.
    OwnerTo { relation: String, owner: String },
    /// `REASSIGN OWNED BY a [, …] TO b` (roles plan R2).
    ReassignOwned { from: Vec<String>, to: String },
    /// `DROP OWNED BY a [, …]`: the roles' grants and default privileges
    /// revoked; refused while they still own objects (REASSIGN them first).
    DropOwned { roles: Vec<String> },
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
    if upper.starts_with("ALL SEQUENCES IN SCHEMA") {
        return Target::AllSequencesInSchema;
    }
    for (prefix, kind) in [("ALL FUNCTIONS IN SCHEMA", "function"), ("ALL PROCEDURES IN SCHEMA", "procedure"), ("ALL ROUTINES IN SCHEMA", "routine")] {
        if upper.starts_with(prefix) {
            return Target::AllRoutinesInSchema { kind };
        }
    }
    // A routine named with its argument types, `f(integer, text)`: the
    // name alone, as routines are kept by name here.
    let names_of = |list: &str| -> Vec<String> {
        crate::core::sql_scan::split_top_level(list, b',')
            .into_iter()
            .map(|n| name_of(n.split('(').next().unwrap_or(n).trim()))
            .filter(|n| !n.is_empty())
            .collect()
    };
    if upper.starts_with("SEQUENCE ") {
        return Target::Sequences(names_of(&t[9..]));
    }
    for (prefix, kind) in [("FUNCTION ", "function"), ("PROCEDURE ", "procedure"), ("ROUTINE ", "routine")] {
        if upper.starts_with(prefix) {
            return Target::Routines { kind, names: names_of(&t[prefix.len()..]) };
        }
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
    if upper.starts_with("REASSIGN OWNED BY ") {
        let body = &text["REASSIGN OWNED BY ".len()..];
        let Some(to) = kw(body, "TO") else {
            return Some(Err(("42601", "syntax error in REASSIGN OWNED: expected TO".to_string())));
        };
        let from = grantee_list(&body[..to]);
        let to = name_of(body[to + 2..].trim());
        if from.is_empty() || to.is_empty() {
            return Some(Err(("42601", "REASSIGN OWNED BY names roles TO a role".to_string())));
        }
        return Some(Ok(PrivilegeStatement::ReassignOwned { from, to }));
    }
    if upper.starts_with("DROP OWNED BY ") {
        let mut body = text["DROP OWNED BY ".len()..].trim();
        for suffix in [" CASCADE", " RESTRICT"] {
            if body.to_ascii_uppercase().ends_with(suffix) {
                body = body[..body.len() - suffix.len()].trim_end();
            }
        }
        let roles = grantee_list(body);
        if roles.is_empty() {
            return Some(Err(("42601", "DROP OWNED BY names roles".to_string())));
        }
        return Some(Ok(PrivilegeStatement::DropOwned { roles }));
    }
    if upper.starts_with("GRANT ") {
        let body = &text[6..];
        let Some(to) = kw(body, "TO") else {
            return Some(Err(("42601", "syntax error in GRANT: expected TO".to_string())));
        };
        let mut grantees_text = body[to + 2..].trim().to_string();
        if let Some(at) = kw(&grantees_text, "GRANTED") {
            grantees_text = grantees_text[..at].trim().to_string();
        }
        let (mut grantable, mut admin, mut inherit, mut set) = (false, false, None, None);
        if let Some(at) = kw(&grantees_text, "WITH") {
            match grant_with(&grantees_text[at + 4..]) {
                Ok(options) => (grantable, admin, inherit, set) = options,
                Err(e) => return Some(Err(e)),
            }
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
            None => Ok(PrivilegeStatement::GrantRole { roles: grantee_list(&body[..to]), members: grantees, admin, inherit, set }),
        });
    }
    if upper.starts_with("REVOKE ") {
        let mut body = &text[7..];
        let mut grant_option_only = false;
        let mut option = None;
        let head = body.to_ascii_uppercase();
        if head.starts_with("GRANT OPTION FOR ") {
            grant_option_only = true;
            body = &body[17..];
        } else {
            // Once skipped and ignored: `REVOKE ADMIN OPTION FOR r FROM u`
            // revoked the whole membership.
            for (prefix, which) in [
                ("ADMIN OPTION FOR ", MembershipOption::Admin),
                ("INHERIT OPTION FOR ", MembershipOption::Inherit),
                ("SET OPTION FOR ", MembershipOption::Set),
            ] {
                if head.starts_with(prefix) {
                    option = Some(which);
                    body = &body[prefix.len()..];
                    break;
                }
            }
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
            Some(_) if option.is_some() => Err(("42601", "an OPTION FOR revoke names roles, not privileges ON an object".to_string())),
            Some(on) => privilege_list(&body[..on]).map(|privileges| PrivilegeStatement::Revoke {
                privileges,
                target: target_of(&body[on + 2..from]),
                grantees,
                grant_option_only,
            }),
            None => Ok(PrivilegeStatement::RevokeRole { roles: grantee_list(&body[..from]), members: grantees, option }),
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
        let kind = match objects.as_str() {
            "TABLES" => DefaultKind::Tables,
            "SEQUENCES" => DefaultKind::Sequences,
            "FUNCTIONS" | "ROUTINES" => DefaultKind::Functions,
            _ => DefaultKind::Other,
        };
        let mut grantees_text = body[sep + if grant { 2 } else { 4 }..].trim().to_string();
        for suffix in ["WITH GRANT OPTION", "CASCADE", "RESTRICT"] {
            if let Some(at) = grantees_text.to_ascii_uppercase().find(suffix) {
                grantees_text.truncate(at);
            }
        }
        let privileges = if kind != DefaultKind::Other {
            match privilege_list(&body[..on]).map(|p| privileges_for(p, kind.privileges())) {
                Ok(Ok(p)) => p,
                Ok(Err(p)) => return Some(Err(("0LP01", format!("invalid privilege type {p} for {}", objects.to_ascii_lowercase())))),
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
            kind,
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
// The catalogue guard
// ---------------------------------------------------------------------------

/// Names in the catalogue's namespace that are not relations: the alias an
/// enum sort's rewrite gives its derived table (`user_types`), and the
/// functions the collation and domain rewrites call.
const NOT_CATALOGUES: &[&str] = &["__rdb_o", "__rdb_ci_key", "__rdb_ci_ai_key", "__rdb_domain_value"];

/// The prefix of the tables the MySQL and SQL Server front ends make for a
/// client's temporary tables (`sqlcompat::TEMP_PREFIX`): the client's own
/// relations, written into its translated statements, not a catalogue.
const FRONT_END_TEMP_PREFIX: &str = "__rdb_tmp_";

/// The projection of `rdb_roles` a client may read, as PostgreSQL's
/// `pg_roles` is readable: every column but the verifier.
pub const ROLE_CATALOG_MASK: &str = "SELECT name, can_login, is_superuser FROM rdb_roles";

/// Whether `name` is one of the engine's catalogues, which no client
/// statement may read or write (memory: rdb-roles-writable-escalation): the
/// role catalogue, and every `__rdb_*` table (ACL, views, sequences,
/// functions, triggers, types, domains, the front ends' routines, the merge
/// key map). A prefix, not a list, so a catalogue added later is covered
/// before anyone remembers to list it. Compared without case: a quoted
/// `"RDB_ROLES"` is refused too rather than argued about.
pub fn is_protected_catalog(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    if name == crate::auth::ROLE_TABLE {
        return true;
    }
    name.starts_with("__rdb_") && !name.starts_with(FRONT_END_TEMP_PREFIX) && !NOT_CATALOGUES.contains(&name.as_str())
}

/// The cheap first test of [`catalog_refusal`]: a statement without the
/// bytes `__rdb_` or `rdb_roles` names no catalogue, and is not scanned.
pub fn may_name_catalog(sql: &str) -> bool {
    crate::core::sql_scan::contains_ignore_ascii_case(sql, b"__rdb_")
        || crate::core::sql_scan::contains_ignore_ascii_case(sql, crate::auth::ROLE_TABLE.as_bytes())
}

fn catalog_denied(name: &str) -> Refusal {
    ("42501", format!("permission denied: \"{name}\" is a system catalog"))
}

/// The refusal for a client's statement that names an engine catalogue
/// ([`is_protected_catalog`]); `None` when it names none, or only reads
/// `rdb_roles` where [`mask_role_catalog`] then hides its verifiers.
///
/// Read by tokens, every identifier wherever it stands, so `COPY`,
/// `TRUNCATE`, `LOCK`, `GRANT … ON`, `ALTER … RENAME TO`, `CREATE INDEX …
/// ON` and DDL meet it as a `SELECT` does: a guard taught each clause would
/// miss the next one. A column qualifier (`t.c`) and an alias (`AS x`) are
/// not relations. `rdb_roles` may be read, and only where every mention of
/// it is a relation read from a `FROM` list, which is exactly where the mask
/// reaches; a statement that writes it, locks it, or names it anywhere else
/// is refused. Fails closed: a form this does not understand is a refusal.
///
/// The engine's own statements on its catalogues never come through here
/// (they run on the executor directly, or in a session marked as the
/// engine's: `SessionState::system_catalog`, `system_statement`).
pub fn catalog_refusal(sql: &str) -> Option<Refusal> {
    use crate::core::mvcc::table_locks::TableMode;
    if !may_name_catalog(sql) {
        return None;
    }
    let words = crate::core::rql::lock_set::named_words(sql);
    let head = words.first().map(|w| w.name.to_ascii_uppercase()).unwrap_or_default();
    let mut role_mentions = 0usize;
    for word in &words {
        if word.qualifier || word.alias || !is_protected_catalog(&word.name) {
            continue;
        }
        if word.name.eq_ignore_ascii_case(crate::auth::ROLE_TABLE) {
            role_mentions += 1;
            continue;
        }
        return Some(catalog_denied(&word.name));
    }
    if role_mentions == 0 {
        return None;
    }
    let role = crate::auth::ROLE_TABLE;
    // Its columns, which `SHOW COLUMNS` lists (Studio's table view) and
    // which say nothing a client could use.
    if head == "SHOW" && words.get(1).is_some_and(|w| w.name.eq_ignore_ascii_case("COLUMNS")) {
        return None;
    }
    // The statements that reach the mask: everything else that could read a
    // relation (`COPY`, `EXPLAIN`, `MERGE`, `CREATE … AS`, `DO`) is answered
    // before it, so it would read the verifiers.
    if !matches!(head.as_str(), "SELECT" | "WITH" | "VALUES" | "INSERT" | "UPDATE" | "DELETE") {
        return Some(catalog_denied(role));
    }
    let reads: Vec<_> = crate::core::rql::lock_set::relation_references(sql)
        .into_iter()
        .filter(|r| r.name.rsplit('.').next().unwrap_or(&r.name).eq_ignore_ascii_case(role))
        .collect();
    if reads.len() != role_mentions || reads.iter().any(|r| r.mode != TableMode::AccessShare) {
        return Some(catalog_denied(role));
    }
    None
}

/// Whether the read of `rdb_roles` at `span` is the mask's own: the whole
/// of `(SELECT name, can_login, is_superuser FROM rdb_roles)`, or a
/// statement that is exactly that. Nothing can be added to either (no
/// `WHERE`, no other column), so a client writing it reads what the mask
/// would give.
fn is_masked_read(sql: &str, span: &std::ops::Range<usize>) -> bool {
    const HEAD: &str = "SELECT name, can_login, is_superuser FROM";
    let (Some(before), Some(after)) = (sql.get(..span.start), sql.get(span.end..)) else {
        return false;
    };
    let before = before.trim_end();
    let Some(cut) = before.len().checked_sub(HEAD.len()) else {
        return false;
    };
    let (Some(rest), Some(tail)) = (before.get(..cut), before.get(cut..)) else {
        return false;
    };
    if !tail.eq_ignore_ascii_case(HEAD) {
        return false;
    }
    let rest = rest.trim_end();
    let after = after.trim_start();
    (rest.ends_with('(') && after.starts_with(')')) || (rest.is_empty() && after.trim_end_matches(';').trim().is_empty())
}

/// `sql` with every read of `rdb_roles` replaced by [`ROLE_CATALOG_MASK`]
/// as a derived table under the same name (or the alias it had), so no
/// statement a client wrote, a view expanded or a function inlined reads a
/// verifier; `None` when it reads none. Run after [`catalog_refusal`] has
/// let the statement through, as the last rewrite before it is planned.
pub fn mask_role_catalog(sql: &str) -> Option<String> {
    use crate::core::mvcc::table_locks::TableMode;
    let role = crate::auth::ROLE_TABLE;
    if !crate::core::sql_scan::contains_ignore_ascii_case(sql, role.as_bytes()) {
        return None;
    }
    let reads: Vec<_> = crate::core::rql::lock_set::relation_references(sql)
        .into_iter()
        .filter(|r| r.mode == TableMode::AccessShare)
        .filter(|r| r.name.rsplit('.').next().unwrap_or(&r.name).eq_ignore_ascii_case(role))
        .filter(|r| !is_masked_read(sql, &r.span))
        .collect();
    if reads.is_empty() {
        return None;
    }
    let mut out = sql.to_string();
    for r in reads.iter().rev() {
        let replacement =
            if r.aliased { format!("({ROLE_CATALOG_MASK})") } else { format!("({ROLE_CATALOG_MASK}) AS {role}") };
        out.replace_range(r.span.clone(), &replacement);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// The permission rows cached per database (roles plan R1, §6): a check no
/// longer runs a `SELECT` of the whole catalogue in a spawned task for every
/// statement of a non-superuser.
///
/// Correct across transactions: a write inside a transaction marks its
/// database *pending* with that transaction; while any pending writer is
/// still running, loads bypass the cache (what a new snapshot sees may
/// change at its commit), and once every pending writer has finished the
/// cache is dropped and refilled. A write outside a transaction drops it at
/// once.
mod cache {
    use std::sync::Arc as StdArc;

    use dashmap::DashMap;

    use super::AclRow;
    use crate::core::mvcc::{TxId, TxOutcome};

    static ROWS: std::sync::LazyLock<DashMap<u64, StdArc<Vec<AclRow>>>> = std::sync::LazyLock::new(DashMap::new);
    static PENDING: std::sync::LazyLock<DashMap<u64, Vec<TxId>>> = std::sync::LazyLock::new(DashMap::new);
    /// Bumped by every write, so a load that raced one does not fill the
    /// cache with what it read before the write.
    static GEN: std::sync::LazyLock<DashMap<u64, u64>> = std::sync::LazyLock::new(DashMap::new);

    pub(super) fn generation(db: u64) -> u64 {
        GEN.get(&db).map_or(0, |g| *g)
    }

    pub(super) fn wrote(db: u64, tx: Option<TxId>) {
        *GEN.entry(db).or_insert(0) += 1;
        match tx {
            Some(t) => {
                let mut p = PENDING.entry(db).or_default();
                if !p.contains(&t) {
                    p.push(t);
                }
                ROWS.remove(&db);
            }
            None => {
                ROWS.remove(&db);
            }
        }
    }

    /// The cached rows, if they may be served: no pending writer is still
    /// running. Finished writers are cleared (and the cache dropped).
    pub(super) fn get(db: &crate::core::database::Database) -> Option<StdArc<Vec<AclRow>>> {
        if let Some(mut p) = PENDING.get_mut(&db.id) {
            let before = p.len();
            p.retain(|t| db.transactions.outcome(*t) == TxOutcome::InProgress);
            if p.len() != before {
                ROWS.remove(&db.id);
            }
            if !p.is_empty() {
                return None;
            }
        }
        ROWS.get(&db.id).map(|r| r.clone())
    }

    pub(super) fn put(db: &crate::core::database::Database, rows: &[AclRow], seen: u64) {
        if PENDING.get(&db.id).is_some_and(|p| !p.is_empty()) || generation(db.id) != seen {
            return;
        }
        ROWS.insert(db.id, StdArc::new(rows.to_vec()));
    }
}

/// A write to the permission catalogue made outside this module (a
/// replicated statement): the cache treats it as its own.
pub fn note_catalogue_write(database: u64, tx: TxId) {
    cache::wrote(database, Some(tx));
}

fn run(
    database: &Arc<Database>,
    sql: &str,
    tx: Option<TxId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
    use crate::core::rql::{executor, lexer_s1::recognize_query_purpose};
    let database = database.clone();
    let sql = sql.to_string();
    if !sql.trim_start().get(..6).is_some_and(|w| w.eq_ignore_ascii_case("SELECT")) {
        cache::wrote(database.id, tx);
    }
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
    // A statement's own transaction may hold uncommitted grants: it reads
    // them itself. Only the committed view is cached.
    if tx.is_none()
        && let Some(rows) = cache::get(database)
    {
        return Ok((*rows).clone());
    }
    let seen = cache::generation(database.id);
    let rows = load_uncached(database, tx).await?;
    if tx.is_none() {
        cache::put(database, &rows, seen);
    }
    Ok(rows)
}

async fn load_uncached(database: &Arc<Database>, tx: Option<TxId>) -> Result<Vec<AclRow>, String> {
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

/// Sets role attribute `attr` of `role` to `value`, or clears it for
/// `None` (`auth::ROLE_ATTR_RELATION`).
pub async fn set_role_attr(database: &Arc<Database>, role: &str, attr: &str, value: Option<String>, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::{ROLE_ATTR_RELATION, quote};
    match value {
        Some(value) => {
            let row = AclRow {
                relation: ROLE_ATTR_RELATION.to_string(),
                grantee: role.to_string(),
                privilege: attr.to_string(),
                grantor: value,
                grantable: false,
            };
            put(database, &row, tx).await
        }
        None => {
            let filter =
                format!("relation = {} AND grantee = {} AND privilege = {}", quote(ROLE_ATTR_RELATION), quote(role), quote(attr));
            delete_where(database, &filter, tx).await
        }
    }
}

/// Every row naming role `from`, as grantee, grantor, member list or
/// default-privilege owner, rewritten to name `to` (`ALTER ROLE … RENAME`).
pub async fn rename_role(database: &Arc<Database>, from: &str, to: &str, tx: Option<TxId>) -> Result<(), String> {
    use crate::auth::{ROLE_ATTR_RELATION, quote};
    let member = format!("@role:{from}");
    let defaults = default_relations(from);
    let renamed_defaults = default_relations(to);
    let rows: Vec<AclRow> = load(database, tx)
        .await?
        .into_iter()
        .filter(|r| r.grantee == from || r.grantor == from || r.relation == member || defaults.contains(&r.relation))
        .collect();
    if rows.is_empty() {
        return Ok(());
    }
    let filter = format!(
        "grantee = {f} OR relation = {m} OR relation = {d0} OR relation = {d1} OR relation = {d2} OR (grantor = {f} AND relation <> {a})",
        f = quote(from),
        m = quote(&member),
        d0 = quote(&defaults[0]),
        d1 = quote(&defaults[1]),
        d2 = quote(&defaults[2]),
        a = quote(ROLE_ATTR_RELATION),
    );
    delete_where(database, &filter, tx).await?;
    let swap = |name: &str| if name == from { to.to_string() } else { name.to_string() };
    for mut r in rows {
        if r.relation == member {
            r.relation = format!("@role:{to}");
        } else if let Some(i) = defaults.iter().position(|d| *d == r.relation) {
            r.relation = renamed_defaults[i].clone();
        }
        r.grantee = swap(&r.grantee);
        // An attribute row's grantor is its value, not a role.
        if r.relation != ROLE_ATTR_RELATION {
            r.grantor = swap(&r.grantor);
        }
        put(database, &r, tx).await?;
    }
    Ok(())
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
// Owners for a database from before them
// ---------------------------------------------------------------------------

/// The row that says a database was given owners ([`assign_owners`]): from
/// then on a relation with no owner is a new one some path did not record,
/// and is given an owner but no grants.
const UPGRADE_MARKER: &str = "@upgrade:r0";

/// The relations a client names and owners are kept for: tables, views and
/// sequences, not the engine's own (`__…`) nor the role catalogue.
async fn owned_relation_names(database: &Arc<Database>) -> Vec<String> {
    let mut names: Vec<String> = database
        .relation_names()
        .into_iter()
        .filter(|n| !n.starts_with("__") && n != crate::auth::ROLE_TABLE)
        .collect();
    names.extend(crate::core::rql::views::load(database, None, None).await.unwrap_or_default().into_iter().map(|v| v.name));
    names.extend(
        crate::core::rql::sequence_catalog::load(database, None, None).await.unwrap_or_default().into_iter().map(|s| s.name),
    );
    names.sort();
    names.dedup();
    names
}

/// Records that `database` needs no legacy upgrade: one made from here on
/// (`CREATE DATABASE`, an import's target), whose relations are owned as
/// they are made.
pub async fn mark_current(database: &Arc<Database>, owner: &str) -> Result<(), String> {
    let marker = AclRow {
        relation: UPGRADE_MARKER.to_string(),
        grantee: owner.to_string(),
        privilege: "UPGRADED".to_string(),
        grantor: owner.to_string(),
        grantable: false,
    };
    put(database, &marker, None).await
}

/// Gives every relation of `database` without an owner one: `owner`, the
/// database's owner (its first superuser, or the bootstrap role). Once per
/// database, the first time this runs (no marker yet), each such relation is
/// also granted to PUBLIC with every table privilege: before owners existed
/// an unowned relation was open to every role, and the grant keeps that, now
/// as rows an owner can `REVOKE`, rather than as a rule that let any role
/// take the relation over (`OWNER TO`) or read it when the ACL failed to
/// load. After the marker a relation found without an owner gets the owner
/// alone, closed to the other roles until granted.
///
/// Returns how many relations it gave an owner. Run where a database's roles
/// are prepared (`auth::ensure_role_catalogue`, pgwire's `seed_roles`).
pub async fn assign_owners(database: &Arc<Database>, owner: &str) -> Result<usize, String> {
    let rows = load(database, None).await?;
    let legacy = !rows.iter().any(|r| r.relation == UPGRADE_MARKER);
    let mut assigned = 0usize;
    for name in owned_relation_names(database).await {
        if owner_of(&rows, &name).is_some() {
            continue;
        }
        let row = AclRow {
            relation: name.clone(),
            grantee: owner.to_string(),
            privilege: "OWNER".to_string(),
            grantor: owner.to_string(),
            grantable: false,
        };
        put(database, &row, None).await?;
        if legacy {
            for privilege in TABLE_PRIVILEGES {
                let grant = AclRow {
                    relation: name.clone(),
                    grantee: "public".to_string(),
                    privilege: privilege.to_string(),
                    grantor: owner.to_string(),
                    grantable: false,
                };
                put(database, &grant, None).await?;
            }
        }
        assigned += 1;
    }
    if legacy {
        mark_current(database, owner).await?;
        if assigned > 0 {
            log::warn!(
                "Database \"{}\": {} relation{} from before owners existed now belong to \"{}\", and PUBLIC keeps \
                 every privilege on them it had; REVOKE ALL ON <table> FROM PUBLIC to close one",
                database.name,
                assigned,
                if assigned == 1 { "" } else { "s" },
                owner
            );
        }
    } else if assigned > 0 {
        log::info!("Database \"{}\": {} relation{} without an owner now belong to \"{}\"", database.name, assigned, if assigned == 1 { "" } else { "s" }, owner);
    }
    Ok(assigned)
}

// ---------------------------------------------------------------------------
// Checking
// ---------------------------------------------------------------------------

/// The roles `role` acts as: itself, and every role it is a member of,
/// through memberships of memberships.
pub fn roles_of(rows: &[AclRow], role: &str) -> Vec<String> {
    reachable(rows, role, NOINHERIT)
}

/// The roles `role` may `SET ROLE` to, itself first: every membership on
/// the way has SET (R2). Who acts as a role (REASSIGN OWNED, DROP OWNED)
/// asks this too, as PostgreSQL does.
pub fn settable_roles(rows: &[AclRow], role: &str) -> Vec<String> {
    reachable(rows, role, NOSET)
}

/// The roles reached from `role` through memberships that carry no `skip`
/// companion row.
fn reachable(rows: &[AclRow], role: &str, skip: &str) -> Vec<String> {
    // Rare: collected once, so a walk with none checks nothing per edge.
    let off: Vec<(&str, &str)> =
        rows.iter().filter(|r| r.privilege == skip).map(|r| (r.relation.as_str(), r.grantee.as_str())).collect();
    let mut out = vec![role.to_string()];
    let mut at = 0;
    while at < out.len() {
        let member = out[at].clone();
        for row in rows.iter().filter(|r| r.privilege == "MEMBER" && r.grantee == member) {
            if off.iter().any(|(rel, g)| *rel == row.relation && *g == row.grantee) {
                continue;
            }
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

/// Whether `actor` holds ADMIN OPTION on `role`: a membership of its own,
/// or of a role it inherits, granted WITH ADMIN.
pub fn has_admin_option(rows: &[AclRow], actor: &str, role: &str) -> bool {
    let relation = format!("@role:{role}");
    let mine = roles_of(rows, actor);
    rows.iter().any(|r| r.privilege == "MEMBER" && r.grantable && r.relation == relation && mine.contains(&r.grantee))
}

/// `relation`'s owner, `None` for one created before owners were recorded.
pub fn owner_of<'a>(rows: &'a [AclRow], relation: &str) -> Option<&'a str> {
    rows.iter().find(|r| r.privilege == "OWNER" && r.relation == relation).map(|r| r.grantee.as_str())
}

/// Whether `role` may use `privilege` on `relation` (its owner may do all).
/// A relation with no owner is nobody's to use but a superuser's, who is not
/// asked: fails closed.
pub fn allows(rows: &[AclRow], role: &str, relation: &str, privilege: &str) -> bool {
    let Some(owner) = owner_of(rows, relation) else {
        return false;
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

/// Whether `role` acts as `relation`'s owner. No owner: nobody does.
pub fn owns(rows: &[AclRow], role: &str, relation: &str) -> bool {
    owner_of(rows, relation).is_some_and(|owner| roles_of(rows, role).iter().any(|r| r == owner))
}

/// Whether `role` may grant `privilege` on `relation`: its owner, or a
/// holder of it `WITH GRANT OPTION`. An unowned relation's privileges are
/// only a superuser's to give.
pub fn may_grant(rows: &[AclRow], role: &str, relation: &str, privilege: &str) -> bool {
    let acting = roles_of(rows, role);
    match owner_of(rows, relation) {
        None => false,
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
            PrivilegeStatement::GrantRole {
                roles: vec!["readers".into()],
                members: vec!["alice".into(), "bob".into()],
                admin: false,
                inherit: None,
                set: None,
            }
        );
        assert_eq!(
            parse_privilege_statement("ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA public GRANT SELECT ON TABLES TO app").unwrap().unwrap(),
            PrivilegeStatement::DefaultPrivileges {
                for_role: Some("owner".into()),
                grant: true,
                privileges: vec!["SELECT".into()],
                grantees: vec!["app".into()],
                kind: DefaultKind::Tables,
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
        // Unowned: nobody's but a superuser's (fails closed).
        assert!(!allows(&rows, "bob", "legacy", "DELETE"));
        assert!(!may_grant(&rows, "bob", "legacy", "SELECT"));
        assert!(!may_grant(&rows, "alice", "t", "SELECT"));
    }
}
