//! The `IMPORT` statement.
//!
//! ```text
//! IMPORT DATABASE FROM POSTGRES 'postgresql://user:pass@host:5432/db'
//!     INTO analytics
//!
//! IMPORT DATABASE FROM POSTGRES
//!     HOST 'db.internal' PORT 5432
//!     DATABASE 'shop' USER 'migrator' PASSWORD 'secret'
//!     SSLMODE 'verify-full'
//!     SCHEMA 'public'
//!     EXCLUDE TABLES (audit_log, sessions)
//!     INTO shop
//! ```
//!
//! Answered above the executor, alongside `CREATE DATABASE`, because it creates
//! a database: the executor is handed one and cannot see the catalogue.

use super::pg_client::{ConnectParams, SslMode};
use super::plan::{ImportOptions, WideNumeric};
use super::types::JsonMapping;

/// A parsed `IMPORT DATABASE`.
#[derive(Debug, Clone)]
pub struct ImportStatement {
    pub source: ConnectParams,
    pub target: String,
    pub options: ImportOptions,
}

/// Whether text begins an `IMPORT` statement.
///
/// Checked before the statement is parsed so that the dispatch does not have to
/// try a parse — and so `IMPORT` said any other way still reaches the ordinary
/// "unsupported statement" reply rather than a confusing import error.
pub fn is_import_statement(sql: &str) -> bool {
    let mut words = sql.split_whitespace();
    words.next().is_some_and(|w| w.eq_ignore_ascii_case("IMPORT"))
}

/// Parses an `IMPORT` statement, or explains what is wrong with it.
pub fn parse(sql: &str) -> Result<ImportStatement, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    let words = tokenize(trimmed);

    let mut index = 0usize;
    if !words
        .first()
        .is_some_and(|w| w.eq_ignore_ascii_case("IMPORT"))
    {
        return Err("not an IMPORT statement".to_string());
    }
    index += 1;

    // `DATABASE` is optional noise that reads better in a runbook.
    if words
        .get(index)
        .is_some_and(|w| w.eq_ignore_ascii_case("DATABASE"))
    {
        index += 1;
    }

    if !words.get(index).is_some_and(|w| w.eq_ignore_ascii_case("FROM")) {
        return Err(USAGE.to_string());
    }
    index += 1;

    match words.get(index).map(|w| w.to_ascii_uppercase()) {
        Some(word) if word == "POSTGRES" || word == "POSTGRESQL" || word == "PG" => index += 1,
        Some(other) => {
            return Err(format!(
                "IMPORT can only read from POSTGRES, not from {}",
                other
            ));
        }
        None => return Err(USAGE.to_string()),
    }

    let mut source = ConnectParams::default();
    let mut options = ImportOptions::default();
    let mut target: Option<String> = None;
    // Whether anything at all said where to connect, so an `IMPORT` with no
    // source is refused rather than quietly dialling localhost.
    let mut source_given = false;

    // A connection URI, if that is the form used.
    if let Some(word) = words.get(index) {
        if is_quoted(word) && looks_like_uri(&unquote(word)) {
            source = ConnectParams::parse_uri(&unquote(word))?;
            source_given = true;
            index += 1;
        }
    }

    while index < words.len() {
        let keyword = words[index].to_ascii_uppercase();
        match keyword.as_str() {
            "HOST" => {
                source.host = required(&words, index + 1, "HOST needs a host name")?;
                source_given = true;
                index += 2;
            }
            "PORT" => {
                let text = required(&words, index + 1, "PORT needs a number")?;
                source.port = text
                    .parse()
                    .map_err(|_| format!("PORT {:?} is not a port number", text))?;
                index += 2;
            }
            "USER" => {
                source.user = required(&words, index + 1, "USER needs a name")?;
                index += 2;
            }
            "PASSWORD" => {
                source.password = Some(required(&words, index + 1, "PASSWORD needs a value")?);
                index += 2;
            }
            "DATABASE" | "DBNAME" => {
                source.database = required(&words, index + 1, "DATABASE needs a name")?;
                source_given = true;
                index += 2;
            }
            "SSLMODE" => {
                let text = required(&words, index + 1, "SSLMODE needs a value")?;
                source.sslmode = SslMode::parse(&text).ok_or_else(|| {
                    format!(
                        "SSLMODE {:?} is not one of disable, prefer, require or verify-full",
                        text
                    )
                })?;
                index += 2;
            }
            "SCHEMA" => {
                options.schema = required(&words, index + 1, "SCHEMA needs a name")?;
                index += 2;
            }
            "ONLY" | "INCLUDE" => {
                let (names, used) = table_list(&words, index + 1, &keyword)?;
                options.include = names;
                index += used + 1;
            }
            "EXCLUDE" => {
                let (names, used) = table_list(&words, index + 1, &keyword)?;
                options.exclude = names;
                index += used + 1;
            }
            "BATCH" => {
                let text = required(&words, index + 1, "BATCH needs a number of rows")?;
                options.batch_rows = text
                    .parse::<usize>()
                    .map_err(|_| format!("BATCH {:?} is not a number of rows", text))?
                    .max(1);
                index += 2;
            }
            "JSON" => {
                // `JSON AS TEXT` / `JSON AS NATIVE`.
                let mut at = index + 1;
                if words.get(at).is_some_and(|w| w.eq_ignore_ascii_case("AS")) {
                    at += 1;
                }
                let text = required(&words, at, "JSON needs AS TEXT or AS NATIVE")?;
                options.json = JsonMapping::parse(&text)
                    .ok_or_else(|| format!("JSON AS {:?}: expected TEXT or NATIVE", text))?;
                index = at + 1;
            }
            "WIDE" => {
                // `WIDE NUMERIC AS TEXT` / `... AS FAIL`.
                let mut at = index + 1;
                if words.get(at).is_some_and(|w| w.eq_ignore_ascii_case("NUMERIC")) {
                    at += 1;
                }
                if words.get(at).is_some_and(|w| w.eq_ignore_ascii_case("AS")) {
                    at += 1;
                }
                let text = required(&words, at, "WIDE NUMERIC needs AS TEXT or AS FAIL")?;
                options.wide_numeric = WideNumeric::parse(&text)
                    .ok_or_else(|| format!("WIDE NUMERIC AS {:?}: expected TEXT or FAIL", text))?;
                index = at + 1;
            }
            "WITHOUT" => {
                if !words
                    .get(index + 1)
                    .is_some_and(|w| w.eq_ignore_ascii_case("CONSTRAINTS"))
                {
                    return Err("WITHOUT must be followed by CONSTRAINTS".to_string());
                }
                options.skip_constraints = true;
                index += 2;
            }
            "DRY" => {
                if !words
                    .get(index + 1)
                    .is_some_and(|w| w.eq_ignore_ascii_case("RUN"))
                {
                    return Err("DRY must be followed by RUN".to_string());
                }
                options.dry_run = true;
                index += 2;
            }
            "INTO" => {
                let name = required(&words, index + 1, "INTO needs a name for the new database")?;
                target = Some(name);
                index += 2;
            }
            other => {
                return Err(format!("unexpected {:?} in IMPORT DATABASE\n\n{}", other, USAGE));
            }
        }
    }

    if !source_given {
        return Err(format!("IMPORT does not say what to read from.\n\n{}", USAGE));
    }
    if source.database.is_empty() {
        return Err("IMPORT does not name a database to read; add DATABASE '<name>'".to_string());
    }
    let target = target.ok_or_else(|| {
        format!(
            "IMPORT does not say what to call the new database; add INTO <name>.\n\n{}",
            USAGE
        )
    })?;

    if options.dry_run && target.is_empty() {
        return Err("INTO needs a non-empty name".to_string());
    }

    Ok(ImportStatement {
        source,
        target,
        options,
    })
}

const USAGE: &str = "\
IMPORT DATABASE FROM POSTGRES '<connection uri>' INTO <new database>

or, spelled out:

IMPORT DATABASE FROM POSTGRES
    HOST '<host>' [PORT <n>] DATABASE '<name>'
    USER '<user>' PASSWORD '<password>' [SSLMODE '<mode>']
    [SCHEMA '<schema>'] [ONLY TABLES (a, b)] [EXCLUDE TABLES (c)]
    [BATCH <rows>] [JSON AS TEXT|NATIVE] [WIDE NUMERIC AS FAIL|TEXT]
    [WITHOUT CONSTRAINTS] [DRY RUN]
    INTO <new database>";

/// Reads `TABLES (a, b, c)`, returning the names and how many words were used.
fn table_list(words: &[String], mut at: usize, keyword: &str) -> Result<(Vec<String>, usize), String> {
    let start = at;
    if words.get(at).is_some_and(|w| w.eq_ignore_ascii_case("TABLES")) {
        at += 1;
    } else if words.get(at).is_some_and(|w| w.eq_ignore_ascii_case("TABLE")) {
        at += 1;
    }

    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut saw_paren = false;

    while at < words.len() {
        let word = &words[at];
        let mut text = word.as_str();

        if text.starts_with('(') {
            depth += 1;
            saw_paren = true;
            text = &text[1..];
        }
        let mut closes = false;
        while text.ends_with(')') {
            closes = true;
            depth = depth.saturating_sub(1);
            text = &text[..text.len() - 1];
        }

        let name = unquote(text.trim().trim_end_matches(','));
        if !name.is_empty() {
            names.push(name);
        }
        at += 1;

        if saw_paren && closes && depth == 0 {
            break;
        }
        if !saw_paren {
            // Without parentheses the list ends at the first word that is not
            // followed by a comma.
            if !word.ends_with(',') {
                break;
            }
        }
    }

    if names.is_empty() {
        return Err(format!("{} TABLES needs at least one table name", keyword));
    }

    Ok((names, at - start))
}

fn required(words: &[String], at: usize, message: &str) -> Result<String, String> {
    words
        .get(at)
        .map(|w| unquote(w))
        .filter(|w| !w.is_empty())
        .ok_or_else(|| message.to_string())
}

fn looks_like_uri(text: &str) -> bool {
    text.starts_with("postgres://") || text.starts_with("postgresql://")
}

fn is_quoted(word: &str) -> bool {
    let bytes = word.as_bytes();
    bytes.len() >= 2
        && (bytes[0] == b'\'' || bytes[0] == b'"')
        && bytes[bytes.len() - 1] == bytes[0]
}

/// Splits on whitespace, keeping quoted strings whole.
///
/// A doubled quote inside a quoted string is one quote, so a password
/// containing one can be written.
fn tokenize(sql: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();

    while let Some(c) = chars.next() {
        match quote {
            Some(q) if c == q => {
                if chars.peek() == Some(&q) {
                    // A doubled quote is one quote, and the string continues.
                    current.push(q);
                    current.push(q);
                    chars.next();
                } else {
                    current.push(c);
                    quote = None;
                }
            }
            Some(_) => current.push(c),
            None if c == '\'' || c == '"' => {
                current.push(c);
                quote = Some(c);
            }
            None if c.is_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            None => current.push(c),
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn unquote(raw: &str) -> String {
    let trimmed = raw.trim().trim_end_matches(',');
    let bytes = trimmed.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        if (first == b'\'' || first == b'"') && bytes[bytes.len() - 1] == first {
            let inner = &trimmed[1..trimmed.len() - 1];
            let doubled = [first as char, first as char].iter().collect::<String>();
            return inner.replace(&doubled, &(first as char).to_string());
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uri_says_everything_the_connection_needs() {
        let statement =
            parse("IMPORT DATABASE FROM POSTGRES 'postgresql://bob:s3cret@db.internal:5433/shop' INTO analytics")
                .unwrap();
        assert_eq!(statement.source.host, "db.internal");
        assert_eq!(statement.source.port, 5433);
        assert_eq!(statement.source.user, "bob");
        assert_eq!(statement.source.password.as_deref(), Some("s3cret"));
        assert_eq!(statement.source.database, "shop");
        assert_eq!(statement.target, "analytics");
        assert_eq!(statement.options.schema, "public");
    }

    #[test]
    fn the_spelled_out_form_says_the_same_thing() {
        let statement = parse(
            "IMPORT DATABASE FROM POSTGRES HOST 'db.internal' PORT 5433 DATABASE 'shop' \
             USER 'bob' PASSWORD 's3cret' INTO analytics",
        )
        .unwrap();
        assert_eq!(statement.source.host, "db.internal");
        assert_eq!(statement.source.port, 5433);
        assert_eq!(statement.source.user, "bob");
        assert_eq!(statement.source.password.as_deref(), Some("s3cret"));
        assert_eq!(statement.source.database, "shop");
        assert_eq!(statement.target, "analytics");
    }

    #[test]
    fn the_short_spellings_are_accepted() {
        for sql in [
            "IMPORT FROM PG 'postgres://u:p@h:5432/d' INTO t",
            "import database from postgresql 'postgres://u:p@h:5432/d' into t;",
        ] {
            assert_eq!(parse(sql).unwrap().target, "t");
        }
    }

    #[test]
    fn a_password_may_contain_anything_a_quoted_string_can() {
        let statement = parse(
            "IMPORT FROM POSTGRES HOST 'h' DATABASE 'd' USER 'u' PASSWORD 'p@ss w''rd:/@' INTO t",
        )
        .unwrap();
        assert_eq!(statement.source.password.as_deref(), Some("p@ss w'rd:/@"));
    }

    #[test]
    fn the_password_never_reaches_the_description() {
        let statement =
            parse("IMPORT FROM POSTGRES 'postgresql://bob:s3cret@h:5432/d' INTO t").unwrap();
        let described = statement.source.redacted();
        assert!(!described.contains("s3cret"), "{}", described);
        assert!(described.contains("bob"), "{}", described);
    }

    #[test]
    fn table_lists_are_read_with_or_without_parentheses() {
        let statement = parse(
            "IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' ONLY TABLES (users, orders) INTO t",
        )
        .unwrap();
        assert_eq!(statement.options.include, vec!["users", "orders"]);

        let statement = parse(
            "IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' EXCLUDE TABLES ('audit_log') INTO t",
        )
        .unwrap();
        assert_eq!(statement.options.exclude, vec!["audit_log"]);

        let statement =
            parse("IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' ONLY TABLES users INTO t")
                .unwrap();
        assert_eq!(statement.options.include, vec!["users"]);
    }

    #[test]
    fn the_options_are_read() {
        let statement = parse(
            "IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' SCHEMA 'sales' BATCH 250 \
             JSON AS NATIVE WIDE NUMERIC AS TEXT WITHOUT CONSTRAINTS DRY RUN INTO t",
        )
        .unwrap();
        assert_eq!(statement.options.schema, "sales");
        assert_eq!(statement.options.batch_rows, 250);
        assert_eq!(statement.options.json, JsonMapping::Native);
        assert_eq!(statement.options.wide_numeric, WideNumeric::Text);
        assert!(statement.options.skip_constraints);
        assert!(statement.options.dry_run);
    }

    #[test]
    fn sslmode_is_read_and_checked() {
        let statement = parse(
            "IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' SSLMODE 'verify-full' INTO t",
        )
        .unwrap();
        assert_eq!(statement.source.sslmode, SslMode::VerifyFull);

        let error =
            parse("IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d' SSLMODE 'maybe' INTO t")
                .unwrap_err();
        assert!(error.contains("verify-full"), "{}", error);
    }

    #[test]
    fn sslmode_in_the_uri_is_read_too() {
        let statement =
            parse("IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d?sslmode=require' INTO t").unwrap();
        assert_eq!(statement.source.sslmode, SslMode::Require);
    }

    #[test]
    fn a_statement_that_says_nothing_useful_explains_itself() {
        assert!(parse("IMPORT").unwrap_err().contains("IMPORT DATABASE FROM POSTGRES"));
        assert!(
            parse("IMPORT FROM MYSQL 'x' INTO t")
                .unwrap_err()
                .contains("only read from POSTGRES")
        );
        assert!(
            parse("IMPORT FROM POSTGRES 'postgres://u:p@h:5432/d'")
                .unwrap_err()
                .contains("INTO")
        );
        assert!(
            parse("IMPORT DATABASE INTO t")
                .unwrap_err()
                .contains("IMPORT DATABASE FROM POSTGRES")
        );
    }

    #[test]
    fn an_import_with_no_source_is_refused_rather_than_dialling_localhost() {
        let error = parse("IMPORT DATABASE FROM POSTGRES USER 'bob' INTO t").unwrap_err();
        assert!(error.contains("does not say what to read from"), "{}", error);
    }

    #[test]
    fn only_import_statements_are_recognised() {
        assert!(is_import_statement("IMPORT DATABASE FROM POSTGRES 'x' INTO y"));
        assert!(is_import_statement("  import from pg 'x' into y"));
        assert!(!is_import_statement("SELECT * FROM imports"));
        assert!(!is_import_statement("CREATE DATABASE importer"));
        assert!(!is_import_statement(""));
    }

    #[test]
    fn a_uri_with_no_database_is_refused() {
        let error = parse("IMPORT FROM POSTGRES 'postgres://u:p@h:5432' INTO t").unwrap_err();
        assert!(error.contains("names no database"), "{}", error);
    }

    #[test]
    fn an_ipv6_host_survives_its_brackets() {
        let statement =
            parse("IMPORT FROM POSTGRES 'postgres://u:p@[2001:db8::1]:5433/d' INTO t").unwrap();
        assert_eq!(statement.source.host, "2001:db8::1");
        assert_eq!(statement.source.port, 5433);
    }

    #[test]
    fn percent_escapes_in_a_uri_are_decoded() {
        let statement =
            parse("IMPORT FROM POSTGRES 'postgres://u:p%40ss%2Fword@h:5432/d' INTO t").unwrap();
        assert_eq!(statement.source.password.as_deref(), Some("p@ss/word"));
    }
}
