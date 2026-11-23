//! Statements about the session rather than the data: `SET`, `SHOW`, `USE`,
//! `KILL`, transaction control, and the system variables connectors read
//! when they connect.

use std::collections::BTreeMap;

use super::lex::{Tok, Token, split_commas};
use super::translate::{MyError, Value, find_top};

/// What a session statement asks for.
#[derive(Debug)]
pub(crate) enum Action {
    /// Run through the translator and the engine.
    Data,
    /// Answer OK and do nothing.
    Ok,
    Use(String),
    Begin {
        read_only: Option<bool>,
        isolation: Option<String>,
    },
    Commit {
        chain: bool,
    },
    Rollback {
        chain: bool,
    },
    /// SAVEPOINT / RELEASE / ROLLBACK TO: sent as written.
    Savepoint(String),
    Set(Vec<Assignment>),
    /// `SET TRANSACTION …`: the next transaction (`session` false) or every
    /// later one.
    SetTransaction {
        session: bool,
        isolation: Option<String>,
        read_only: Option<bool>,
    },
    Show(Show),
    Kill {
        id: u32,
        query_only: bool,
    },
    Error(MyError),
}

#[derive(Debug)]
pub(crate) enum Target {
    System(String),
    User(String),
    Names(String),
}

#[derive(Debug)]
pub(crate) struct Assignment {
    pub(crate) target: Target,
    pub(crate) value: Vec<Token>,
}

#[derive(Debug)]
pub(crate) enum Show {
    Databases,
    Tables {
        full: bool,
        database: Option<String>,
    },
    Columns {
        full: bool,
        table: String,
    },
    Index {
        table: String,
    },
    CreateTable {
        table: String,
    },
    CreateDatabase {
        name: String,
    },
    Variables,
    Status,
    Warnings,
    CountWarnings,
    Engines,
    Charset,
    Collation,
    Processlist,
    Grants,
    TableStatus,
    /// A listing MySQL has and this server has nothing for.
    Empty(Vec<&'static str>),
}

/// A SHOW's LIKE pattern or WHERE condition.
#[derive(Debug, Default)]
pub(crate) struct Filter {
    pub(crate) like: Option<String>,
    pub(crate) condition: Option<Vec<Token>>,
}

fn word(tokens: &[Token], i: usize) -> String {
    tokens.get(i).and_then(|t| t.upper()).unwrap_or_default()
}

fn ident(tokens: &[Token], i: usize) -> Option<String> {
    tokens.get(i).and_then(|t| t.ident()).map(|s| s.to_string())
}

/// `db.t` or `t`, from `i`: the table and the index after it.
fn table_at(tokens: &[Token], i: usize) -> Option<(String, usize)> {
    let first = ident(tokens, i)?;
    if tokens.get(i + 1).is_some_and(|t| t.is_op(".")) {
        return Some((ident(tokens, i + 2)?, i + 3));
    }
    Some((first, i + 1))
}

fn filter_from(tokens: &[Token], from: usize) -> Filter {
    let mut filter = Filter::default();
    if let Some(at) = find_top(tokens, from, &["LIKE"]) {
        if let Some(Tok::Str(s)) = tokens.get(at + 1).map(|t| &t.tok) {
            filter.like = Some(String::from_utf8_lossy(s).into_owned());
        }
    } else if let Some(at) = find_top(tokens, from, &["WHERE"]) {
        filter.condition = Some(tokens[at + 1..].to_vec());
    }
    filter
}

fn isolation_of(tokens: &[Token], from: usize) -> Option<String> {
    let at = (from..tokens.len()).find(|&i| word(tokens, i) == "ISOLATION")?;
    let a = word(tokens, at + 2);
    let b = word(tokens, at + 3);
    Some(
        match (a.as_str(), b.as_str()) {
            ("READ", "UNCOMMITTED") => "READ UNCOMMITTED",
            ("READ", "COMMITTED") => "READ COMMITTED",
            ("REPEATABLE", "READ") => "REPEATABLE READ",
            ("SERIALIZABLE", _) => "SERIALIZABLE",
            _ => return None,
        }
        .to_string(),
    )
}

fn read_only_of(tokens: &[Token], from: usize) -> Option<bool> {
    (from..tokens.len()).find_map(|i| {
        if word(tokens, i) == "READ" {
            match word(tokens, i + 1).as_str() {
                "ONLY" => Some(true),
                "WRITE" => Some(false),
                _ => None,
            }
        } else {
            None
        }
    })
}

/// What a statement is, if it is about the session; `Action::Data` if it is
/// for the translator.
pub(crate) fn classify(tokens: &[Token]) -> (Action, Filter) {
    let head = word(tokens, 0);
    let none = Filter::default();
    match head.as_str() {
        "USE" => match ident(tokens, 1) {
            Some(db) => (Action::Use(db), none),
            None => (
                Action::Error(MyError::syntax("USE needs a database name")),
                none,
            ),
        },
        "BEGIN" if tokens.len() == 1 || word(tokens, 1) == "WORK" => (
            Action::Begin {
                read_only: None,
                isolation: None,
            },
            none,
        ),
        "START" if word(tokens, 1) == "TRANSACTION" => (
            Action::Begin {
                read_only: read_only_of(tokens, 2),
                isolation: None,
            },
            none,
        ),
        "COMMIT" => (
            Action::Commit {
                chain: tokens
                    .iter()
                    .enumerate()
                    .any(|(i, t)| t.is_word("CHAIN") && !word(tokens, i - 1).eq("NO")),
            },
            none,
        ),
        "ROLLBACK" if !tokens.iter().any(|t| t.is_word("TO")) => (
            Action::Rollback {
                chain: tokens
                    .iter()
                    .enumerate()
                    .any(|(i, t)| t.is_word("CHAIN") && !word(tokens, i - 1).eq("NO")),
            },
            none,
        ),
        "ROLLBACK" | "SAVEPOINT" | "RELEASE" => {
            // ROLLBACK [WORK] TO [SAVEPOINT] x, SAVEPOINT x, RELEASE SAVEPOINT x
            let name = tokens
                .last()
                .and_then(|t| t.ident())
                .unwrap_or("")
                .to_lowercase();
            let sql = match head.as_str() {
                "ROLLBACK" => format!(
                    "ROLLBACK TO SAVEPOINT {}",
                    super::translate::ident_sql(&name)
                ),
                "SAVEPOINT" => format!("SAVEPOINT {}", super::translate::ident_sql(&name)),
                _ => format!("RELEASE SAVEPOINT {}", super::translate::ident_sql(&name)),
            };
            (Action::Savepoint(sql), none)
        }
        "SET" => (classify_set(tokens), none),
        "SHOW" => classify_show(tokens),
        "DESCRIBE" | "DESC" | "EXPLAIN"
            if tokens.len() >= 2
                && !matches!(
                    word(tokens, 1).as_str(),
                    "SELECT"
                        | "INSERT"
                        | "UPDATE"
                        | "DELETE"
                        | "REPLACE"
                        | "WITH"
                        | "FORMAT"
                        | "EXTENDED"
                        | "ANALYZE"
                        | "PARTITIONS"
                ) =>
        {
            match table_at(tokens, 1) {
                Some((table, _)) => (Action::Show(Show::Columns { full: false, table }), none),
                None => (Action::Data, none),
            }
        }
        "KILL" => {
            let mut i = 1;
            let mut query_only = false;
            if word(tokens, 1) == "QUERY" {
                query_only = true;
                i = 2;
            } else if word(tokens, 1) == "CONNECTION" {
                i = 2;
            }
            match tokens.get(i).map(|t| &t.tok) {
                Some(Tok::Num(n)) => match n.parse() {
                    Ok(id) => (Action::Kill { id, query_only }, none),
                    Err(_) => (
                        Action::Error(MyError::new(
                            1094,
                            "HY000",
                            format!("Unknown thread id: {n}"),
                        )),
                        none,
                    ),
                },
                _ => (
                    Action::Error(MyError::syntax("KILL needs a connection id")),
                    none,
                ),
            }
        }
        // Accepted and ignored: table locks (sessions isolate through
        // transactions here), caches, statistics, and the binary log.
        "LOCK" | "UNLOCK" | "FLUSH" | "ANALYZE" | "OPTIMIZE" | "CHECK" | "REPAIR" | "PURGE"
        | "RESET" => (Action::Ok, none),
        "DO" => (Action::Ok, none),
        "GRANT" | "REVOKE" => (
            Action::Error(MyError::unsupported(
                "GRANT and REVOKE in MySQL's form (use CREATE ROLE / ALTER ROLE)",
            )),
            none,
        ),
        // CALL and LOAD DATA are run by the session (conn.rs) before this;
        // here they are only classified for a prepared statement.
        "CALL" => (Action::Ok, none),
        "PREPARE" | "EXECUTE" | "DEALLOCATE" | "HANDLER" | "LOAD" | "XA" => {
            (Action::Error(MyError::unsupported(head)), none)
        }
        _ => (Action::Data, none),
    }
}

fn classify_set(tokens: &[Token]) -> Action {
    let w1 = word(tokens, 1);
    // SET [SESSION|GLOBAL] TRANSACTION …
    let (scope_session, at) = match w1.as_str() {
        "SESSION" | "LOCAL" => (true, 2),
        "GLOBAL" | "PERSIST" => (true, 2),
        _ => (false, 1),
    };
    if word(tokens, at) == "TRANSACTION" {
        return Action::SetTransaction {
            session: scope_session,
            isolation: isolation_of(tokens, at),
            read_only: read_only_of(tokens, at),
        };
    }
    if w1 == "PASSWORD" {
        return Action::Error(MyError::unsupported(
            "SET PASSWORD (use ALTER ROLE … PASSWORD)",
        ));
    }
    let mut assignments = Vec::new();
    for part in split_commas(&tokens[1..]) {
        let mut p = 0usize;
        let mut scope_seen = false;
        while matches!(
            word(&part, p).as_str(),
            "SESSION" | "LOCAL" | "GLOBAL" | "PERSIST" | "PERSIST_ONLY"
        ) {
            p += 1;
            scope_seen = true;
        }
        let _ = scope_seen;
        let first = match part.get(p) {
            Some(t) => t,
            None => return Action::Error(MyError::syntax("SET needs an assignment")),
        };
        // NAMES / CHARACTER SET / CHARSET
        if first.is_word("NAMES") {
            let cs = part.get(p + 1).map(set_word).unwrap_or_default();
            assignments.push(Assignment {
                target: Target::Names(cs),
                value: Vec::new(),
            });
            continue;
        }
        if first.is_word("CHARSET") || (first.is_word("CHARACTER") && word(&part, p + 1) == "SET") {
            let at = if first.is_word("CHARSET") {
                p + 1
            } else {
                p + 2
            };
            let cs = part.get(at).map(set_word).unwrap_or_default();
            assignments.push(Assignment {
                target: Target::Names(cs),
                value: Vec::new(),
            });
            continue;
        }
        let (target, eq) = match &first.tok {
            Tok::UserVar(name) => (Target::User(name.clone()), p + 1),
            Tok::SysVar(name) => (Target::System(name.clone()), p + 1),
            Tok::Word(name) | Tok::Quoted(name) => {
                // `session.x` / `@@session.x` spelled with a dot.
                if part.get(p + 1).is_some_and(|t| t.is_op(".")) {
                    match ident(&part, p + 2) {
                        Some(n) => (Target::System(n.to_lowercase()), p + 3),
                        None => {
                            return Action::Error(MyError::syntax("a variable name was expected"));
                        }
                    }
                } else {
                    (Target::System(name.to_lowercase()), p + 1)
                }
            }
            _ => return Action::Error(MyError::syntax("a variable name was expected")),
        };
        if !part.get(eq).is_some_and(|t| t.is_op("=") || t.is_op(":=")) {
            return Action::Error(MyError::syntax("SET needs '='"));
        }
        assignments.push(Assignment {
            target,
            value: part[eq + 1..].to_vec(),
        });
    }
    Action::Set(assignments)
}

fn set_word(t: &Token) -> String {
    match &t.tok {
        Tok::Word(w) | Tok::Quoted(w) => w.to_lowercase(),
        Tok::Str(s) => String::from_utf8_lossy(s).to_lowercase(),
        _ => String::new(),
    }
}

fn classify_show(tokens: &[Token]) -> (Action, Filter) {
    let mut i = 1usize;
    let mut full = false;
    // SHOW [FULL|EXTENDED] …, SHOW [GLOBAL|SESSION] VARIABLES
    loop {
        match word(tokens, i).as_str() {
            "FULL" => {
                full = true;
                i += 1;
            }
            "EXTENDED" | "GLOBAL" | "SESSION" | "LOCAL" => i += 1,
            _ => break,
        }
    }
    let what = word(tokens, i);
    let filter = filter_from(tokens, i);
    let show = match what.as_str() {
        "DATABASES" | "SCHEMAS" => Show::Databases,
        "TABLES" => {
            let database = if matches!(word(tokens, i + 1).as_str(), "FROM" | "IN") {
                ident(tokens, i + 2)
            } else {
                None
            };
            Show::Tables { full, database }
        }
        "COLUMNS" | "FIELDS" => {
            if !matches!(word(tokens, i + 1).as_str(), "FROM" | "IN") {
                return (
                    Action::Error(MyError::syntax("SHOW COLUMNS needs FROM table")),
                    filter,
                );
            }
            match table_at(tokens, i + 2) {
                Some((table, _)) => Show::Columns { full, table },
                None => {
                    return (
                        Action::Error(MyError::syntax("SHOW COLUMNS needs a table")),
                        filter,
                    );
                }
            }
        }
        "INDEX" | "INDEXES" | "KEYS" => match table_at(tokens, i + 2) {
            Some((table, _)) => Show::Index { table },
            None => {
                return (
                    Action::Error(MyError::syntax("SHOW INDEX needs a table")),
                    filter,
                );
            }
        },
        "CREATE" => match word(tokens, i + 1).as_str() {
            "TABLE" => match table_at(tokens, i + 2) {
                Some((table, _)) => Show::CreateTable { table },
                None => {
                    return (
                        Action::Error(MyError::syntax("SHOW CREATE TABLE needs a table")),
                        filter,
                    );
                }
            },
            "DATABASE" | "SCHEMA" => {
                let mut at = i + 2;
                if word(tokens, at) == "IF" {
                    at += 3;
                }
                match ident(tokens, at) {
                    Some(name) => Show::CreateDatabase { name },
                    None => {
                        return (
                            Action::Error(MyError::syntax("SHOW CREATE DATABASE needs a name")),
                            filter,
                        );
                    }
                }
            }
            other => {
                return (
                    Action::Error(MyError::unsupported(format!("SHOW CREATE {other}"))),
                    filter,
                );
            }
        },
        "VARIABLES" => Show::Variables,
        "STATUS" => Show::Status,
        "WARNINGS" | "ERRORS" => Show::Warnings,
        "COUNT" => Show::CountWarnings,
        "ENGINES" | "STORAGE" => Show::Engines,
        "CHARACTER" | "CHARSET" => Show::Charset,
        "COLLATION" => Show::Collation,
        "PROCESSLIST" => Show::Processlist,
        "GRANTS" => Show::Grants,
        "TABLE" if word(tokens, i + 1) == "STATUS" => Show::TableStatus,
        "TRIGGERS" => Show::Empty(vec![
            "Trigger",
            "Event",
            "Table",
            "Statement",
            "Timing",
            "Created",
            "sql_mode",
            "Definer",
        ]),
        "EVENTS" => Show::Empty(vec!["Db", "Name", "Definer", "Time zone", "Type"]),
        "PLUGINS" => Show::Empty(vec!["Name", "Status", "Type", "Library", "License"]),
        "PROCEDURE" | "FUNCTION" => {
            Show::Empty(vec!["Db", "Name", "Type", "Definer", "Modified", "Created"])
        }
        "MASTER" | "BINARY" | "REPLICA" | "SLAVE" | "BINLOG" | "OPEN" | "PRIVILEGES"
        | "PROFILES" => Show::Empty(vec!["Value"]),
        other => {
            return (
                Action::Error(MyError::unsupported(format!("SHOW {other}"))),
                filter,
            );
        }
    };
    (Action::Show(show), filter)
}

/// MySQL's LIKE, case-insensitively (SHOW patterns are).
pub(crate) fn like_matches(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.first() {
            None => t.is_empty(),
            Some('%') => (0..=t.len()).any(|k| go(&p[1..], &t[k..])),
            Some('_') => !t.is_empty() && go(&p[1..], &t[1..]),
            Some('\\') if p.len() > 1 => t.first() == Some(&p[1]) && go(&p[2..], &t[1..]),
            Some(c) => t.first().is_some_and(|x| x.eq_ignore_ascii_case(c)) && go(&p[1..], &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

/// The variables a session starts with.
pub(crate) fn initial_variables(tls: bool, port: u16) -> BTreeMap<String, Value> {
    let s = |v: &str| Value::Str(v.to_string());
    let i = Value::Int;
    let mut m = BTreeMap::new();
    for (k, v) in [
        ("auto_increment_increment", i(1)),
        ("auto_increment_offset", i(1)),
        ("autocommit", i(1)),
        ("binlog_format", s("ROW")),
        ("character_set_client", s("utf8mb4")),
        ("character_set_connection", s("utf8mb4")),
        ("character_set_database", s("utf8mb4")),
        ("character_set_filesystem", s("binary")),
        ("character_set_results", s("utf8mb4")),
        ("character_set_server", s("utf8mb4")),
        ("character_set_system", s("utf8mb3")),
        ("collation_connection", s("utf8mb4_bin")),
        ("collation_database", s("utf8mb4_bin")),
        ("collation_server", s("utf8mb4_bin")),
        ("default_storage_engine", s("InnoDB")),
        ("default_week_format", i(0)),
        ("div_precision_increment", i(4)),
        ("explicit_defaults_for_timestamp", i(1)),
        ("foreign_key_checks", i(1)),
        ("group_concat_max_len", i(1024)),
        ("have_ssl", s(if tls { "YES" } else { "DISABLED" })),
        ("have_openssl", s(if tls { "YES" } else { "DISABLED" })),
        ("have_query_cache", s("NO")),
        ("init_connect", s("")),
        ("innodb_lock_wait_timeout", i(50)),
        ("interactive_timeout", i(28800)),
        ("license", s("GPL")),
        ("local_infile", i(1)),
        ("lock_wait_timeout", i(31536000)),
        ("log_bin", i(0)),
        ("lower_case_table_names", i(1)),
        (
            "max_allowed_packet",
            i(super::packet::MAX_ALLOWED_PACKET as i64),
        ),
        ("max_execution_time", i(0)),
        ("net_buffer_length", i(16384)),
        ("net_read_timeout", i(30)),
        ("net_write_timeout", i(60)),
        ("performance_schema", i(0)),
        ("port", i(port as i64)),
        ("protocol_version", i(10)),
        ("query_cache_size", i(0)),
        ("query_cache_type", s("OFF")),
        ("read_only", i(0)),
        ("require_secure_transport", i(0)),
        ("server_id", i(1)),
        ("session_track_schema", i(0)),
        ("session_track_state_change", i(0)),
        ("session_track_system_variables", s("")),
        ("session_track_transaction_info", s("OFF")),
        ("sql_auto_is_null", i(0)),
        (
            "sql_mode",
            s(
                "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
            ),
        ),
        ("sql_notes", i(1)),
        ("sql_quote_show_create", i(1)),
        ("sql_safe_updates", i(0)),
        (
            "sql_select_limit",
            Value::Str("18446744073709551615".into()),
        ),
        ("sql_warnings", i(0)),
        ("storage_engine", s("InnoDB")),
        ("super_read_only", i(0)),
        ("system_time_zone", s("UTC")),
        ("time_zone", s("SYSTEM")),
        ("transaction_isolation", s("READ-COMMITTED")),
        ("transaction_read_only", i(0)),
        ("tx_isolation", s("READ-COMMITTED")),
        ("tx_read_only", i(0)),
        ("unique_checks", i(1)),
        ("version", s(super::SERVER_VERSION)),
        ("version_comment", s("RasterizedDB (MySQL protocol)")),
        ("version_compile_machine", s(std::env::consts::ARCH)),
        ("version_compile_os", s(std::env::consts::OS)),
        ("wait_timeout", i(28800)),
    ] {
        m.insert(k.to_string(), v);
    }
    m
}

/// A value assigned to a boolean variable (`ON`, `1`, `'true'`) as 1 or 0.
pub(crate) fn as_switch(value: &Value) -> Option<i64> {
    match value {
        Value::Int(n) => Some((*n != 0) as i64),
        Value::Str(s) => match s.to_ascii_uppercase().as_str() {
            "ON" | "TRUE" | "1" | "YES" => Some(1),
            "OFF" | "FALSE" | "0" | "NO" => Some(0),
            _ => None,
        },
        Value::Null => None,
    }
}

/// The isolation level a variable names (`READ-COMMITTED`), in the engine's
/// words.
pub(crate) fn isolation_words(value: &str) -> Option<&'static str> {
    Some(
        match value.to_ascii_uppercase().replace(['-', '_'], " ").as_str() {
            "READ UNCOMMITTED" => "READ UNCOMMITTED",
            "READ COMMITTED" => "READ COMMITTED",
            "REPEATABLE READ" => "REPEATABLE READ",
            "SERIALIZABLE" => "SERIALIZABLE",
            _ => return None,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mysql::lex::{Mode, lex};

    fn classify_sql(sql: &str) -> Action {
        classify(&lex(sql.as_bytes(), Mode::default()).unwrap()).0
    }

    #[test]
    fn session_statements() {
        assert!(matches!(classify_sql("USE app"), Action::Use(db) if db == "app"));
        assert!(matches!(
            classify_sql("START TRANSACTION READ ONLY"),
            Action::Begin {
                read_only: Some(true),
                ..
            }
        ));
        assert!(matches!(
            classify_sql("COMMIT"),
            Action::Commit { chain: false }
        ));
        assert!(
            matches!(classify_sql("ROLLBACK TO SAVEPOINT a"), Action::Savepoint(s) if s == "ROLLBACK TO SAVEPOINT a")
        );
        assert!(
            matches!(classify_sql("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"), Action::Set(a) if matches!(&a[0].target, Target::Names(n) if n == "utf8mb4"))
        );
        assert!(matches!(classify_sql("SET autocommit=0, @x = 1"), Action::Set(a) if a.len() == 2));
        assert!(matches!(
            classify_sql("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"),
            Action::SetTransaction {
                session: true,
                isolation: Some(_),
                ..
            }
        ));
        assert!(matches!(
            classify_sql("SHOW FULL TABLES FROM app LIKE 'a%'"),
            Action::Show(Show::Tables {
                full: true,
                database: Some(_)
            })
        ));
        assert!(matches!(
            classify_sql("DESCRIBE t"),
            Action::Show(Show::Columns { .. })
        ));
        assert!(matches!(
            classify_sql("KILL QUERY 12"),
            Action::Kill {
                id: 12,
                query_only: true
            }
        ));
        assert!(matches!(classify_sql("SELECT 1"), Action::Data));
    }

    #[test]
    fn like_patterns() {
        assert!(like_matches("char%", "character_set_client"));
        assert!(like_matches("t_1", "TA1"));
        assert!(!like_matches("a%", "ba"));
    }
}
