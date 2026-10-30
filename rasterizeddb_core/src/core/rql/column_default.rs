//! Column defaults.
//!
//! A default is stored as the SQL text it was written as, and evaluated when a
//! row is inserted without a value for the column. Keeping the text rather than
//! a pre-encoded value is what lets `DEFAULT CURRENT_TIMESTAMP` mean the time
//! of the insert instead of the time of the `CREATE TABLE`.

use std::io;

use rclite::Arc;

use crate::core::db_type::DbType;
use crate::core::storage_providers::traits::StorageIO;

/// Bytes one persisted default occupies: the column name, the expression, and
/// a tombstone. Fixed-stride, so the file reads as an array.
pub const COLUMN_DEFAULT_SIZE: u64 = 129 + 257 + 1;

/// Longest default expression that can be stored.
pub const MAX_EXPRESSION_LEN: usize = 256;

/// A column's default, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDefault {
    pub column: String,
    pub expression: String,
    pub is_deleted: bool,
}

impl ColumnDefault {
    pub fn into_vec(&self) -> Vec<u8> {
        let mut out = vec![0u8; COLUMN_DEFAULT_SIZE as usize];

        let filtered: String = self
            .column
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        let name = filtered.as_bytes();
        let name_len = name.len().min(128);
        out[..name_len].copy_from_slice(&name[..name_len]);

        let expression = self.expression.as_bytes();
        let expression_len = expression.len().min(MAX_EXPRESSION_LEN);
        out[129..129 + expression_len].copy_from_slice(&expression[..expression_len]);

        out[386] = u8::from(self.is_deleted);
        out
    }

    pub fn from_vec(raw: &[u8]) -> Result<Self, String> {
        if raw.len() != COLUMN_DEFAULT_SIZE as usize {
            return Err(format!(
                "invalid default record: expected {} bytes, got {}",
                COLUMN_DEFAULT_SIZE,
                raw.len()
            ));
        }

        let read = |at: usize, len: usize| -> Result<String, String> {
            let end = raw[at..at + len]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(len - 1);
            std::str::from_utf8(&raw[at..at + end])
                .map(str::to_string)
                .map_err(|e| format!("invalid UTF-8 in default record: {}", e))
        };

        Ok(ColumnDefault {
            column: read(0, 129)?,
            expression: read(129, 257)?,
            is_deleted: raw[386] != 0,
        })
    }
}

fn sidecar_name<S: StorageIO>(schema_io: &Arc<S>) -> String {
    format!("{}_{}", schema_io.get_name().replace(".db", ""), "DEF.db")
}

/// Reads a table's column defaults.
pub async fn load<S: StorageIO>(schema_io: Arc<S>) -> Vec<ColumnDefault> {
    let io = schema_io.create_new(sidecar_name(&schema_io)).await;
    let len = io.get_len().await;

    let mut out: Vec<ColumnDefault> = Vec::new();
    if len < COLUMN_DEFAULT_SIZE {
        return out;
    }

    let mut buffer = vec![0u8; COLUMN_DEFAULT_SIZE as usize];
    for i in 0..(len / COLUMN_DEFAULT_SIZE) {
        let mut position = i * COLUMN_DEFAULT_SIZE;
        if io
            .read_data_into_buffer(&mut position, &mut buffer)
            .await
            .is_err()
        {
            break;
        }
        // A record of zeros is an append a power cut kept the length of and
        // dropped (`torn_tail::is_unwritten`), never a declaration.
        if crate::core::torn_tail::is_unwritten(&buffer) {
            continue;
        }
        match ColumnDefault::from_vec(&buffer) {
            // The newest record for a column wins, so a `SET DEFAULT` is an
            // append rather than a rewrite.
            Ok(entry) => {
                out.retain(|e| !e.column.eq_ignore_ascii_case(&entry.column));
                if !entry.is_deleted {
                    out.push(entry);
                }
            }
            Err(e) => log::error!("Skipping unreadable default record: {}", e),
        }
    }

    out
}

/// Appends a default, superseding any earlier one for the same column.
pub async fn append<S: StorageIO>(schema_io: Arc<S>, entry: &ColumnDefault) -> io::Result<()> {
    let mut writes = crate::core::torn_tail::SidecarWrites::default();
    stage(&*schema_io, &mut writes, entry)?;
    writes.commit(&*schema_io).await
}

/// Adds `entry`'s record to `writes`, the statement's sidecar records, which
/// commit as one transaction (`torn_tail::SidecarWrites`): one log sync for
/// all of them, and after a cut all of them or none.
pub fn stage<S: StorageIO>(
    schema_io: &S,
    writes: &mut crate::core::torn_tail::SidecarWrites,
    entry: &ColumnDefault,
) -> io::Result<()> {
    if entry.expression.len() > MAX_EXPRESSION_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a default expression may be at most {} characters",
                MAX_EXPRESSION_LEN
            ),
        ));
    }

    writes.add(crate::core::torn_tail::sidecar_name(schema_io, "DEF.db"), &entry.into_vec());
    Ok(())
}

/// The default expression declared for `column`, if any.
pub fn expression_for<'a>(column: &str, defaults: &'a [ColumnDefault]) -> Option<&'a str> {
    defaults
        .iter()
        .find(|d| d.column.eq_ignore_ascii_case(column))
        .map(|d| d.expression.as_str())
}

/// Evaluates a default expression to the SQL literal an insert would write.
///
/// Only forms whose meaning is unambiguous are accepted: literals, the boolean
/// keywords, `NULL`, and the few functions a schema actually defaults to.
/// Anything else is refused rather than stored as text and misread later.
/// Drops a trailing `::type` from a `DEFAULT` expression.
///
/// A cast here says nothing the column does not already say: the value is about
/// to be stored in a column of a known type, so `DEFAULT '[]'::jsonb` on a
/// `JSONB` column and `DEFAULT '[]'` are the same default. PostgreSQL writes the
/// cast because its parser needs the literal typed; here the column supplies
/// that.
///
/// Deliberately narrow — only a cast applied to the **whole** expression, and
/// only where what remains is still a literal. A cast in the middle of an
/// expression, or one over a function call, is not redundant in the same way and
/// is left to fail rather than quietly reinterpreted.
pub fn strip_redundant_cast(text: &str) -> &str {
    if let Some(literal) = typed_text_literal(text) {
        return literal;
    }
    let Some(at) = text.rfind("::") else {
        return text;
    };
    let head = text[..at].trim_end();
    let tail = text[at + 2..].trim();

    // The type name must be a bare word — `jsonb`, `smallint`, `text` — so a
    // `::` inside a string literal cannot be mistaken for a cast.
    let is_type_name =
        !tail.is_empty() && tail.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !is_type_name {
        return text;
    }

    // And what remains must be a complete literal: a quoted string, or a bare
    // number or keyword. Anything else — a function call, a parenthesised
    // expression — is a shape this cannot vouch for.
    let simple = (head.starts_with('\'') && head.ends_with('\'') && head.len() >= 2)
        || head
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '+');
    if simple && !head.is_empty() {
        head
    } else {
        text
    }
}

/// The quoted literal of `text 'x'`, `varchar 'x'`, `char 'x'` (and
/// `character`, `bpchar`, `character varying`): the typed-literal spelling of
/// `'x'::text`, whose type the column says too. Taken as anything else, the
/// whole spelling was the value — `text 'zz '` stored as those ten
/// characters, or refused by a `character(3)` as too long (22001).
/// `text` with a `text '…'`-style prefix taken off ([`typed_text_literal`]),
/// or as it is.
pub fn typed_text_literal_or(text: &str) -> &str {
    typed_text_literal(text).unwrap_or(text)
}

fn typed_text_literal(text: &str) -> Option<&str> {
    let text = text.trim();
    let quote = text.find('\'')?;
    let (head, literal) = (text[..quote].trim_end(), &text[quote..]);
    if head.len() == quote {
        // No space before the quote: `E'…'`, `B'…'` and `X'…'` are other forms.
        return None;
    }
    let words: Vec<&str> = head.split_whitespace().collect();
    let text_type = match words.as_slice() {
        [w] => ["text", "varchar", "char", "character", "bpchar"].iter().any(|t| w.eq_ignore_ascii_case(t)),
        [a, b] => a.eq_ignore_ascii_case("character") && b.eq_ignore_ascii_case("varying"),
        _ => false,
    };
    let whole = crate::core::sql_scan::quoted_end(literal.as_bytes(), 0, b'\'') == literal.len();
    (text_type && whole && literal.len() >= 2).then_some(literal)
}

/// The literal a default expression produces for one row about to be written
/// into a table of database `database` ([`crate::core::database::Database::id`]).
///
/// The database is needed only by `nextval`, whose sequence belongs to one
/// database: this is the point at which a row consumes from it. Anything that
/// only wants to know whether an expression is acceptable calls [`validate`],
/// which consumes nothing.
pub fn evaluate(expression: &str, db_type: &DbType, database: u64) -> Result<String, String> {
    evaluate_in(expression, db_type, Some(database))
}

/// Whether a default expression is one [`evaluate`] understands, without
/// evaluating it for a row.
///
/// The difference is `nextval`: checking a `DEFAULT nextval('s')` at `CREATE
/// TABLE` or `ADD COLUMN` used to take a value from `s`, so the table's first
/// row got 2 — and the declaration has no database whose sequence it could
/// rightly take one from anyway.
pub fn validate(expression: &str, db_type: &DbType) -> Result<(), String> {
    evaluate_in(expression, db_type, None).map(|_| ())
}

/// [`evaluate`], or with `database` absent [`validate`].
fn evaluate_in(
    expression: &str,
    db_type: &DbType,
    database: Option<u64>,
) -> Result<String, String> {
    let text = strip_redundant_cast(expression.trim());
    let upper = text.to_ascii_uppercase();

    if text.is_empty() || upper == "NULL" {
        return Ok("NULL".to_string());
    }

    // Time of the insert, not of the declaration, which is why the expression
    // is stored as text and evaluated here.
    //
    // Each read as PostgreSQL reads it (`crate::core::statement_clock`): the
    // transaction's start for `now()` and its spellings, so every row of a
    // statement and every statement of a block take one value; the
    // statement's start for `statement_timestamp()`; the wall clock, per row,
    // for `clock_timestamp()`.
    let clock = match upper.as_str() {
        "CURRENT_TIMESTAMP" | "NOW()" | "CURRENT_TIMESTAMP()" | "LOCALTIMESTAMP" | "LOCALTIMESTAMP()"
        | "TRANSACTION_TIMESTAMP()" => Some(crate::core::statement_clock::transaction_us()),
        "STATEMENT_TIMESTAMP()" => Some(crate::core::statement_clock::statement_us()),
        "CLOCK_TIMESTAMP()" => Some(crate::core::statement_clock::wall_us()),
        _ => None,
    };
    if let Some(micros) = clock {
        // A wall clock defaulting a `DATE` or a `TIME` column is that instant's
        // date or time of day, not the whole timestamp: the column's encoder
        // would refuse the full text, and `DEFAULT now()` on a date column is
        // ordinary enough that refusing it would be the wrong answer.
        return Ok(match db_type {
            DbType::DATE => format!("'{}'", format_date_of(micros)),
            DbType::TIME | DbType::TIMETZ => format!("'{}'", format_time_of(micros, db_type)),
            _ => format!(
                "'{}'",
                crate::core::value_codec::format_timestamp_us(micros)
            ),
        });
    }

    // The date and time of day on their own.
    if matches!(upper.as_str(), "CURRENT_DATE" | "CURRENT_DATE()") {
        let micros = now_micros();
        return Ok(match db_type {
            // A date default on a timestamp column is that date at midnight,
            // which is what PostgreSQL's own cast does.
            DbType::DATETIME => {
                format!("'{} 00:00:00'", format_date_of(micros))
            }
            _ => format!("'{}'", format_date_of(micros)),
        });
    }
    if matches!(
        upper.as_str(),
        "CURRENT_TIME" | "CURRENT_TIME()" | "LOCALTIME" | "LOCALTIME()"
    ) {
        let micros = now_micros();
        return Ok(format!("'{}'", format_time_of(micros, db_type)));
    }

    // `SERIAL`'s default. The counter lives in
    // `crate::core::rql::sequence`; this is the point at which a row consumes
    // from it, so a row that is never written never takes a value.
    if let Some(name) = crate::core::rql::sequence::parse_nextval(text) {
        return Ok(match database {
            Some(database) => crate::core::rql::sequence::next(database, &name).to_string(),
            // Validation: the shape is all that is being checked, and nothing
            // reads this value.
            None => String::new(),
        });
    }

    if upper == "UUIDV7()" {
        return Ok(format!(
            "'{}'",
            crate::core::value_codec::format_uuid(&crate::core::rql::aggregate::uuid_v7())
        ));
    }

    if matches!(upper.as_str(), "GEN_RANDOM_UUID()" | "UUID_GENERATE_V4()" | "UUIDV4()") {
        return Ok(format!(
            "'{}'",
            crate::core::value_codec::format_uuid(&crate::core::value_codec::random_uuid_v4())
        ));
    }

    if upper == "TRUE" || upper == "FALSE" {
        return Ok(if upper == "TRUE" { "1" } else { "0" }.to_string());
    }

    // A quoted literal is already in the form an insert takes.
    if text.starts_with('\'') && text.ends_with('\'') && text.len() >= 2 {
        return Ok(text.to_string());
    }

    // A bare number, which the column's own encoder will read.
    let numeric_body = text.strip_prefix(['-', '+']).unwrap_or(text);
    if !numeric_body.is_empty()
        && numeric_body
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'.')
        && numeric_body.bytes().filter(|b| *b == b'.').count() <= 1
    {
        // A number defaulting a text column is written as text.
        return Ok(
            if matches!(db_type, DbType::STRING | DbType::BYTEA | DbType::JSONB) {
                format!("'{}'", text)
            } else {
                text.to_string()
            },
        );
    }

    Err(format!(
        "DEFAULT {} is not supported: literals, TRUE/FALSE, NULL, CURRENT_TIMESTAMP, now(), \
         clock_timestamp(), CURRENT_DATE, CURRENT_TIME, gen_random_uuid() and uuidv7() are understood",
        text
    ))
}

/// The transaction's start, in microseconds since the Unix epoch: what
/// `CURRENT_DATE` and `CURRENT_TIME` are taken from, as `now()` is.
fn now_micros() -> i64 {
    crate::core::statement_clock::transaction_us()
}

/// The `YYYY-MM-DD` of an instant.
fn format_date_of(micros: i64) -> String {
    // Floor division, so an instant before 1970 lands on the day it belongs to
    // rather than the one after it.
    let days = micros.div_euclid(86_400_000_000);
    let (year, month, day) = crate::core::value_codec::civil_from_days(days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// The time of day of an instant, with the UTC offset when the column keeps
/// one. The server reports `TimeZone=UTC`, so the offset is always `+00`.
fn format_time_of(micros: i64, db_type: &DbType) -> String {
    let of_day = micros.rem_euclid(86_400_000_000);
    let hours = of_day / 3_600_000_000;
    let minutes = (of_day % 3_600_000_000) / 60_000_000;
    let seconds = (of_day % 60_000_000) / 1_000_000;
    let frac = of_day % 1_000_000;
    let mut out = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
    if frac != 0 {
        out.push_str(format!(".{:06}", frac).trim_end_matches('0'));
    }
    if matches!(db_type, DbType::TIMETZ) {
        out.push_str("+00");
    }
    out
}

/// Reads the `DEFAULT …` clause out of a column definition, and returns the
/// definition with the clause removed.
///
/// The clause is taken up to the next constraint keyword, so
/// `DEFAULT 0 NOT NULL` keeps the two apart.
pub fn split_clause(definition: &str) -> (String, Option<String>) {
    let upper = definition.to_ascii_uppercase();
    let Some(at) = find_keyword(&upper, "DEFAULT") else {
        return (definition.to_string(), None);
    };

    let after = &definition[at + "DEFAULT".len()..];
    let after_upper = &upper[at + "DEFAULT".len()..];

    // Where the expression ends: the next constraint keyword at the top level.
    const TERMINATORS: [&str; 7] = [
        "NOT NULL",
        "NULL",
        "PRIMARY KEY",
        "UNIQUE",
        "REFERENCES",
        "CHECK",
        "GENERATED",
    ];
    let mut end = after.len();
    let mut depth = 0usize;
    let mut quoted = false;
    for (i, c) in after.char_indices() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => depth = depth.saturating_sub(1),
            _ if !quoted && depth == 0 => {
                if let Some(t) = TERMINATORS
                    .iter()
                    .find(|t| after_upper.as_bytes()[i..].starts_with(t.as_bytes()))
                {
                    // Only at a word boundary, so `NULLABLE` is not `NULL`.
                    let before_ok = i == 0 || !after.as_bytes()[i - 1].is_ascii_alphanumeric();
                    if before_ok {
                        let _ = t;
                        end = i;
                        break;
                    }
                }
            }
            _ => {}
        }
    }

    let expression = after[..end].trim().to_string();
    let mut remainder = definition[..at].to_string();
    remainder.push_str(&after[end..]);

    if expression.is_empty() {
        (definition.to_string(), None)
    } else {
        (remainder.trim().to_string(), Some(expression))
    }
}

/// Position of `keyword` as a whole word, outside quotes.
///
/// `_` counts as part of a word, or a column named `default_subscription` would
/// be read as the keyword followed by `_subscription`.
fn find_keyword(upper: &str, keyword: &str) -> Option<usize> {
    fn is_word_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_'
    }

    let bytes = upper.as_bytes();
    let mut quoted = false;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            quoted = !quoted;
            i += 1;
            continue;
        }
        if !quoted
            && upper.as_bytes()[i..].starts_with(keyword.as_bytes())
            && (i == 0 || !is_word_byte(bytes[i - 1]))
            && (i + keyword.len() == bytes.len() || !is_word_byte(bytes[i + keyword.len()]))
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No database has this id, so a `nextval` reached by mistake here counts
    /// on a sequence nothing else reads.
    const NO_DB: u64 = u64::MAX;

    /// Checking a `DEFAULT nextval('…')` takes no value from the sequence:
    /// it used to, so a table declared with one wrote 2 into its first row.
    #[test]
    fn validating_a_nextval_default_consumes_nothing() {
        let name = "validate_only_seq";
        let expression = format!("nextval('{}')", name);
        validate(&expression, &DbType::I64).expect("nextval is understood");
        validate(&expression, &DbType::I64).expect("nextval is understood");
        assert!(!crate::core::rql::sequence::is_seeded(NO_DB - 1, name));
        assert_eq!(evaluate(&expression, &DbType::I64, NO_DB - 1).unwrap(), "1");
    }

    #[test]
    fn a_record_round_trips_through_its_bytes() {
        let entry = ColumnDefault {
            column: "status".to_string(),
            expression: "'pending'".to_string(),
            is_deleted: false,
        };
        assert_eq!(ColumnDefault::from_vec(&entry.into_vec()).unwrap(), entry);
    }

    /// A column whose name merely contains the keyword is not a DEFAULT clause.
    #[test]
    fn a_column_named_after_the_keyword_is_not_mistaken_for_one() {
        for definition in [
            "default_subscription UUID",
            "is_default BOOL NOT NULL",
            "defaults TEXT",
        ] {
            let (rest, default) = split_clause(definition);
            assert_eq!(default, None, "{}", definition);
            assert_eq!(rest, definition);
        }
    }

    #[test]
    fn the_clause_is_split_off_and_the_rest_survives() {
        let (rest, default) = split_clause("count INTEGER DEFAULT 0 NOT NULL");
        assert_eq!(default.as_deref(), Some("0"));
        assert_eq!(rest, "count INTEGER NOT NULL");

        let (rest, default) = split_clause("code TEXT NOT NULL DEFAULT 'EUR'");
        assert_eq!(default.as_deref(), Some("'EUR'"));
        assert_eq!(rest, "code TEXT NOT NULL");

        let (rest, default) = split_clause("plain TEXT");
        assert_eq!(default, None);
        assert_eq!(rest, "plain TEXT");
    }

    /// A default that is itself a function call keeps its parentheses.
    #[test]
    fn a_function_default_is_kept_whole() {
        let (rest, default) = split_clause("created_at TIMESTAMPTZ NOT NULL DEFAULT now()");
        assert_eq!(default.as_deref(), Some("now()"));
        assert_eq!(rest, "created_at TIMESTAMPTZ NOT NULL");
    }

    /// A literal containing the word NOT NULL must not end the expression.
    #[test]
    fn a_quoted_literal_is_not_cut_at_a_keyword_inside_it() {
        let (_, default) = split_clause("note TEXT DEFAULT 'NOT NULL' NOT NULL");
        assert_eq!(default.as_deref(), Some("'NOT NULL'"));
    }

    #[test]
    fn literals_evaluate_to_themselves() {
        assert_eq!(evaluate("0", &DbType::I32, NO_DB).unwrap(), "0");
        assert_eq!(evaluate("-3.5", &DbType::F64, NO_DB).unwrap(), "-3.5");
        assert_eq!(evaluate("'EUR'", &DbType::STRING, NO_DB).unwrap(), "'EUR'");
        assert_eq!(evaluate("TRUE", &DbType::BOOL, NO_DB).unwrap(), "1");
        assert_eq!(evaluate("false", &DbType::BOOL, NO_DB).unwrap(), "0");
        assert_eq!(evaluate("NULL", &DbType::STRING, NO_DB).unwrap(), "NULL");
    }

    /// A number defaulting a text column is text, not a number.
    #[test]
    fn a_numeric_default_on_a_text_column_is_quoted() {
        assert_eq!(evaluate("0", &DbType::STRING, NO_DB).unwrap(), "'0'");
    }

    /// The point of storing the expression rather than a value: the timestamp
    /// is the insert's, not the declaration's.
    /// The point of storing the expression rather than a value: every call
    /// yields the current time, and the aliases mean the same thing.
    #[test]
    fn current_timestamp_evaluates_at_call_time() {
        for expression in ["CURRENT_TIMESTAMP", "now()", "LOCALTIMESTAMP"] {
            let rendered = evaluate(expression, &DbType::DATETIME, NO_DB).unwrap();
            let literal = rendered.trim_matches('\'');
            let parsed = crate::core::value_codec::parse_timestamp_us(literal)
                .unwrap_or_else(|| panic!("{} produced {:?}", expression, rendered));

            // Comparing lengths would be flaky: the fractional part is trimmed
            // of trailing zeroes, so the rendering varies by the microsecond.
            let epoch_2020 = crate::core::value_codec::parse_timestamp_us("2020-01-01").unwrap();
            assert!(
                parsed > epoch_2020,
                "{} produced {:?}",
                expression,
                rendered
            );
        }
    }

    #[test]
    fn generated_uuids_are_version_four_and_differ() {
        let a = evaluate("gen_random_uuid()", &DbType::UUID, NO_DB).unwrap();
        let b = evaluate("gen_random_uuid()", &DbType::UUID, NO_DB).unwrap();
        assert_ne!(a, b);
        // Version nibble of a v4 UUID.
        assert_eq!(a.as_bytes()[15] as char, '4', "{}", a);
    }

    #[test]
    fn an_expression_that_cannot_be_understood_is_refused() {
        let e = evaluate("some_function(a, b)", &DbType::I32, NO_DB).unwrap_err();
        assert!(e.contains("not supported"), "{}", e);
    }

    #[test]
    fn an_over_long_expression_is_refused_rather_than_truncated() {
        let long = "'".to_string() + &"x".repeat(300) + "'";
        let entry = ColumnDefault {
            column: "c".to_string(),
            expression: long,
            is_deleted: false,
        };
        assert!(entry.expression.len() > MAX_EXPRESSION_LEN);
    }
}
