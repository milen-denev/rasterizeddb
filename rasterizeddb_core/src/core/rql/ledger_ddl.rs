//! The statement shapes of ledger tables and vaults
//! (docs/ledger-and-vault-plan.md §3.1, §4.2). GPL syntax only: what the
//! clauses mean is the installed hooks' (`ledger_hooks::create_policy`).
//!
//! ```sql
//! CREATE LEDGER TABLE [IF NOT EXISTS] name ( … ) <ledger clauses>
//! CREATE VAULT [IF NOT EXISTS] name <vault clauses>
//! ```
//!
//! [`split`] turns either into the plain `CREATE TABLE` the engine runs, the
//! kind its header records, and the clause text for the hooks.

use crate::core::row::schema::SpecialTable;

/// A ledger or vault `CREATE`, taken apart.
#[derive(Debug, PartialEq, Eq)]
pub struct SpecialCreate {
    pub kind: SpecialTable,
    /// The table's name, as written.
    pub name: String,
    /// The `CREATE TABLE` statement to run.
    pub create_sql: String,
    /// The clauses after the definition, as written.
    pub clauses: String,
}

/// The fixed shape of a vault: one row per event, never updated. A new
/// secret version is a row with `version` counting up per `name` and the
/// sealed value in `value` (base64 of nonce, ciphertext and tag: ciphertext
/// only, ever). A state change (`disabled`, `active`, `deleted`) is a row
/// with `version` 0 and no value. The newest row of a name says its state.
/// Times are microseconds since the epoch, 0 for none.
pub const VAULT_COLUMNS: &str = "(\
    name TEXT NOT NULL, \
    version BIGINT NOT NULL, \
    value TEXT NOT NULL, \
    state TEXT NOT NULL, \
    content_type TEXT NOT NULL, \
    not_before BIGINT NOT NULL, \
    expires_at BIGINT NOT NULL, \
    tags TEXT NOT NULL, \
    created_by TEXT NOT NULL, \
    created_at BIGINT NOT NULL, \
    __ledger_ts BIGINT NOT NULL DEFAULT 0)";

/// Strips `word` (case-insensitive, then whitespace) from the front of `s`.
fn keyword<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let head = s.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    let rest = &s[word.len()..];
    if !rest.is_empty() && !rest.starts_with(|c: char| c.is_whitespace() || c == '(') {
        return None;
    }
    Some(rest.trim_start())
}

/// The byte length of the identifier at the front of `s` (bare or
/// double-quoted, possibly `schema.`-qualified).
fn identifier_len(s: &str) -> Option<usize> {
    let mut at = 0;
    loop {
        let rest = &s[at..];
        if rest.starts_with('"') {
            // `""` is an escaped quote inside a quoted name.
            let mut i = 1;
            let bytes = rest.as_bytes();
            loop {
                match bytes.get(i) {
                    None => return None,
                    Some(b'"') if bytes.get(i + 1) == Some(&b'"') => i += 2,
                    Some(b'"') => break,
                    Some(_) => i += 1,
                }
            }
            at += i + 1;
        } else {
            let n = rest
                .char_indices()
                .find(|(_, c)| !(c.is_alphanumeric() || *c == '_' || *c == '$'))
                .map_or(rest.len(), |(i, _)| i);
            if n == 0 {
                return None;
            }
            at += n;
        }
        if s[at..].starts_with('.') {
            at += 1;
            continue;
        }
        return Some(at);
    }
}

/// The byte offset just past the parenthesis that closes the one `s`
/// starts with, skipping quoted text and nested parentheses.
fn closing_paren(s: &str) -> Option<usize> {
    let mut depth = 0usize;
    let mut quote: Option<char> = None;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match quote {
            Some(q) => {
                if c == q {
                    // A doubled quote is an escaped one.
                    if chars.peek().map(|(_, n)| *n) == Some(q) {
                        chars.next();
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => {
                    depth = depth.checked_sub(1)?;
                    if depth == 0 {
                        return Some(i + 1);
                    }
                }
                _ => {}
            },
        }
    }
    None
}

/// Takes apart a `CREATE LEDGER TABLE` or `CREATE VAULT`; `None` for any
/// other statement, an error for one of these that does not parse.
pub fn split(sql: &str) -> Option<Result<SpecialCreate, String>> {
    let s = sql.trim().trim_end_matches(';').trim_end();
    let rest = keyword(s, "CREATE")?;
    if let Some(rest) = keyword(rest, "LEDGER").and_then(|r| keyword(r, "TABLE")) {
        return Some(ledger(rest));
    }
    if let Some(rest) = keyword(rest, "VAULT") {
        return Some(vault(rest));
    }
    None
}

fn if_not_exists(rest: &str) -> (&'static str, &str) {
    match keyword(rest, "IF").and_then(|r| keyword(r, "NOT")).and_then(|r| keyword(r, "EXISTS")) {
        Some(r) => ("IF NOT EXISTS ", r),
        None => ("", rest),
    }
}

fn ledger(rest: &str) -> Result<SpecialCreate, String> {
    let (ine, rest) = if_not_exists(rest);
    let name_len = identifier_len(rest).ok_or("CREATE LEDGER TABLE needs a table name")?;
    let name = &rest[..name_len];
    let body = rest[name_len..].trim_start();
    if !body.starts_with('(') {
        return Err("CREATE LEDGER TABLE needs a column list".to_string());
    }
    let end = closing_paren(body).ok_or("CREATE LEDGER TABLE: the column list is not closed")?;
    let clauses = body[end..].trim();
    if clauses.is_empty() {
        return Err("CREATE LEDGER TABLE needs NO DELETE UNTIL … AFTER INSERT: a ledger table's retention".to_string());
    }
    // The hidden insert time, last: stamped by the engine on every insert
    // (`ledger_hooks::LEDGER_TS`); the default only makes every insert path
    // carry the column for the stamp to overwrite.
    let columns = &body[..end - 1];
    Ok(SpecialCreate {
        kind: SpecialTable::Ledger,
        name: name.to_string(),
        create_sql: format!(
            "CREATE TABLE {ine}{name} {columns}, {} BIGINT NOT NULL DEFAULT 0)",
            crate::core::ledger_hooks::LEDGER_TS
        ),
        clauses: clauses.to_string(),
    })
}

fn vault(rest: &str) -> Result<SpecialCreate, String> {
    let (ine, rest) = if_not_exists(rest);
    let name_len = identifier_len(rest).ok_or("CREATE VAULT needs a name")?;
    let name = &rest[..name_len];
    let clauses = rest[name_len..].trim();
    if clauses.starts_with('(') {
        return Err("a vault's columns are fixed: CREATE VAULT takes no column list".to_string());
    }
    Ok(SpecialCreate {
        kind: SpecialTable::Vault,
        name: name.to_string(),
        create_sql: format!("CREATE TABLE {ine}{name} {VAULT_COLUMNS}"),
        clauses: clauses.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_ledger_create_splits_into_the_table_and_its_clauses() {
        let c = split(
            "create ledger table if not exists pay (id INT, note TEXT DEFAULT ')(', \"we\"\"ird\" INT) \
             NO DELETE UNTIL 7 YEARS AFTER INSERT ANCHOR TO 'dir:/x';",
        )
        .unwrap()
        .unwrap();
        assert_eq!(c.kind, SpecialTable::Ledger);
        assert_eq!(
            c.create_sql,
            "CREATE TABLE IF NOT EXISTS pay (id INT, note TEXT DEFAULT ')(', \"we\"\"ird\" INT, __ledger_ts BIGINT NOT NULL DEFAULT 0)"
        );
        assert_eq!(c.clauses, "NO DELETE UNTIL 7 YEARS AFTER INSERT ANCHOR TO 'dir:/x'");
    }

    #[test]
    fn a_ledger_create_needs_a_retention_and_a_vault_takes_no_columns() {
        assert!(split("CREATE LEDGER TABLE t (a INT)").unwrap().is_err());
        assert!(split("CREATE VAULT v (a INT)").unwrap().is_err());
        let v = split("CREATE VAULT app_secrets RETAIN DELETED 90 DAYS").unwrap().unwrap();
        assert_eq!(v.kind, SpecialTable::Vault);
        assert!(v.create_sql.starts_with("CREATE TABLE app_secrets (name TEXT NOT NULL"));
        assert_eq!(v.clauses, "RETAIN DELETED 90 DAYS");
    }

    #[test]
    fn other_statements_are_not_taken() {
        assert!(split("CREATE TABLE t (a INT)").is_none());
        assert!(split("CREATE LEDGERS").is_none());
        assert!(split("CREATE VAULTED t").is_none());
    }

    #[test]
    fn multibyte_names_and_values_do_not_cut() {
        let c = split("CREATE LEDGER TABLE plaçé (n TEXT DEFAULT 'ü)') NO DELETE UNTIL 1 DAY AFTER INSERT")
            .unwrap()
            .unwrap();
        assert_eq!(c.create_sql, "CREATE TABLE plaçé (n TEXT DEFAULT 'ü)', __ledger_ts BIGINT NOT NULL DEFAULT 0)");
    }
}
